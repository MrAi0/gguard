// Package mysql holds the small slice of the MySQL client/server protocol the
// proxy needs: packet framing and command identification.
package mysql

import (
	"errors"
	"io"
)

// HeaderLen is the fixed header on every MySQL wire packet:
// 3 bytes of payload length (little-endian) followed by a 1 byte sequence id.
const HeaderLen = 4

// Packet is a single MySQL wire packet, header included, exactly as it
// appeared on the wire so it can be forwarded untouched.
type Packet []byte

// Seq returns the packet's sequence id.
func (p Packet) Seq() byte { return p[3] }

// Payload returns the packet body without the header.
func (p Packet) Payload() []byte { return p[HeaderLen:] }

// ReadPacket reads exactly one packet from r. It returns io.EOF only when the
// stream ends cleanly between packets; a stream cut anywhere inside a packet
// yields io.ErrUnexpectedEOF and no partial packet.
func ReadPacket(r io.Reader) (Packet, error) {
	var header [HeaderLen]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	pkt := make(Packet, HeaderLen+payloadLen(header))
	copy(pkt, header[:])

	if _, err := io.ReadFull(r, pkt[HeaderLen:]); err != nil {
		// ReadFull reports io.EOF when zero payload bytes arrived, but having
		// read a header, that is still a truncated packet.
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return pkt, nil
}

func payloadLen(header [HeaderLen]byte) int {
	return int(header[0]) | int(header[1])<<8 | int(header[2])<<16
}
