package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/MrAi0/goguard/proxy"
)

func main() {
	localAddr := ":8080"
	remoteAddr := "127.0.0.1:3306"

	p := proxy.New(localAddr, remoteAddr)

	log.Printf("Database proxy started on %s, forwarding to %s\n", localAddr, remoteAddr)

	go p.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Println("Shutting down proxy..")
}
