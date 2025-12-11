package internal

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Command struct{
	cmd string
	args []string
}


func RedisonnHandler(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Client connected:", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
        if err != nil {
			fmt.Println("Client disconnected:", err)
			return
		}
		command := ParseCommands(msg)

		switch command.cmd{
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		
		case "ECHO":
			conn.Write([]byte(""+strings.Join(command.args, " ")+"\r\n"))
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}


func ParseCommands(msg string)Command{
	fields := strings.Fields(msg)
	if len(fields) == 0{
		return Command{}
	}
	cmd := strings.ToUpper(fields[0])
	args := []string{}
	if len(fields) > 1{
		args = fields[1:]
	}
	return Command{
		cmd,
		args,
	}
}