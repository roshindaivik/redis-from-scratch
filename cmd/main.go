package main

import (
	"fmt"
	"net"
	"os"

	internal "github.com/roshindaivik/redis-from-scratch/handler"
)


func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	fmt.Println("Listening on port 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go internal.RedisConnHandler(conn)
	}
}
