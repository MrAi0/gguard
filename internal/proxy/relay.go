package proxy

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"

	"github.com/MrAi0/goguard/internal/mysql"
)

// relay copies MySQL packets from src to dst one whole packet at a time,
// calling inspect (if non-nil) on each packet before it is forwarded. It
// returns the first read or write error; io.EOF means src closed cleanly.
func relay(dst io.Writer, src io.Reader, inspect func(mysql.Packet)) error {
	r := bufio.NewReader(src)

	for {
		pkt, err := mysql.ReadPacket(r)
		if err != nil {
			return err
		}

		if inspect != nil {
			inspect(pkt)
		}

		if _, err := dst.Write(pkt); err != nil {
			return err
		}
	}
}

func logRelayExit(direction string, err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return
	}
	log.Printf("%s relay stopped: %v", direction, err)
}
