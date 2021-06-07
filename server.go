package ToyRPC

import (
	"ToyRPC/codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0xabcdef

const (
	connected        = "200 Connected to RPC"
	defaultRPCPath   = "/prc"
	defaultDebugPath = "/debug/rpc"
)

type Option struct {
	MagicNumber    int // marks
	CodecType      codec.Type
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// Accept accepts connections on the listener and serves requests for each incoming connection.
func (s *Server) Accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("rpc main: accept error:", err)
			return
		}
		go s.ServeConn(conn)
	}
}

func Accept(listener net.Listener) {
	DefaultServer.Accept(listener)
}

// ServeConn blocks,serving the connection until the client hangs up
func (s *Server) ServeConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	var opt Option
	// decode option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc main: options error: ", err)
		return
	}
	// check the magic number
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc main: invalid magic number %x\n", opt.MagicNumber)
		return
	}
	// get the encode/decode function
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc main: invalid codec type %s\n", opt.CodecType)
		return
	}
	s.serveCodec(f(conn), &opt)
}

var invalidRequest = struct{}{}

func (s *Server) serveCodec(code codec.Codec, opt *Option) {
	sending := new(sync.Mutex) // make sure to send just a complete response
	wg := new(sync.WaitGroup)  // wait until the requests are handled
	for {
		req, err := s.readRequest(code) // read the request
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			s.sendResponse(code, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1) // done a request
		go s.handleRequest(code, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = code.Close()
}

type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

func (s *Server) readRequest(code codec.Codec) (*request, error) {
	h, err := s.readRequestHeader(code)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mtype, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = code.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

func (s *Server) readRequestHeader(code codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := code.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc main: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (s *Server) sendResponse(code codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := code.Write(h, body); err != nil {
		log.Println("rpc main: write response error:", err)
	}
}

func (s *Server) handleRequest(code codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(code, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		s.sendResponse(code, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		s.sendResponse(code, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

func (s *Server) Register(rcvr interface{}) error {
	service := newService(rcvr)
	if _, dup := s.serviceMap.LoadOrStore(service.name, service); dup {
		return errors.New("rpc: service already defined: " + service.name)
	}
	return nil
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

func (s *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ":", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.ServeConn(conn)

}

func (s *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, s)
	http.Handle(defaultDebugPath, debugHTTP{s})
	log.Println("rpc server debug path: " + defaultDebugPath)
}

func HandleHttp() {
	DefaultServer.HandleHTTP()
}
