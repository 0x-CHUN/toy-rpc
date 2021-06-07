package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	connection io.ReadWriteCloser
	buf        *bufio.Writer // prevent block using buffed writer
	decoder    *gob.Decoder
	encoder    *gob.Encoder
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		connection: conn,
		buf:        buf,
		decoder:    gob.NewDecoder(conn),
		encoder:    gob.NewEncoder(buf),
	}
}

func (c *GobCodec) Close() error {
	return c.connection.Close()
}

func (c *GobCodec) ReadHeader(header *Header) error {
	return c.decoder.Decode(header)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.decoder.Decode(body)
}

func (c *GobCodec) Write(header *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()

	if err := c.encoder.Encode(header); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.encoder.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

var _ Codec = (*GobCodec)(nil)
