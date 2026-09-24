package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/MrAi0/goguard/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":8080", "address to accept client connections on")
	upstreamAddr := flag.String("upstream", "127.0.0.1:3306", "MySQL server to forward connections to")
	logQueries := flag.Bool("log-queries", true, "log each client command and its SQL text")
	logPackets := flag.Bool("log-packets", false, "log direction, sequence id and length of every packet")
	flag.Parse()

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to start listener on %s: %v", *listenAddr, err)
	}

	p := proxy.New(proxy.Config{
		UpstreamAddr: *upstreamAddr,
		LogQueries:   *logQueries,
		LogPackets:   *logPackets,
	})

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c

		log.Println("Shutting down proxy..")
		ln.Close()
	}()

	log.Printf("Database proxy started on %s, forwarding to %s", ln.Addr(), *upstreamAddr)
	p.Serve(ln)
}
