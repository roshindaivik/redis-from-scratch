package internal

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ValueType int

const (
	STRING ValueType = iota
	LIST 
)

type Value struct{
	Data interface{}
	Expiry *time.Time 
	Type ValueType	
}

type DB struct {
	mu sync.Mutex
	kv map[string]Value
}

type Command struct {
	cmd  string
	args []string
}

var db = DB{
	kv: make(map[string]Value),
}


func RedisConnHandler(conn net.Conn) {
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
		if command.cmd == "" {
			conn.Write([]byte("-ERR empty command\r\n"))
			continue
		}

		switch command.cmd {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "SET":
			if err := db.SetKey(command.args); err != nil {
				conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			} else {
				conn.Write([]byte("+OK\r\n"))
			}
		case "GET":
			val, ok := db.GetKey(command.args)
			if !ok {
				conn.Write([]byte("nil\r\n")) // Redis style nil
			} else {
				conn.Write([]byte(val+"\r\n"))
			}
		case "ECHO":
			if len(command.args) == 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'ECHO'\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("+%s\r\n", strings.Join(command.args, " "))))
			}
		default:
			conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command.cmd)))
		}
	}
}

func (db *DB) GetKey(args []string) (string, bool) {
	if len(args) < 1 {
		return "", false
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.kv[args[0]]
	if val.Expiry != nil && time.Now().After(*val.Expiry){
		delete(db.kv,args[0])
		return "",false
	}
	return val.Data.(string), ok
}

func (db *DB) SetKey(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("wrong number of arguments for 'SET'")
	}
	key := args[0]
	value := args[1]

	var expiry *time.Time;

	if len(args) > 2 && args[2] == "PX"{
		if len(args) < 4{
			return fmt.Errorf("syntax error")
		}
		// Somehow set a timer for it to expire 
		ms, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil{
			return fmt.Errorf("invalid expire time in 'SET'")
		}
		expiryTime := time.Now().Add(time.Duration(ms) * time.Second)
		expiry = &expiryTime	
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.kv[key] = Value{
		Data: value,
		Expiry: expiry,	
		Type: STRING,
	}
	fmt.Println(db)
	return nil
}

func ParseCommands(msg string) Command {
	fields := strings.Fields(strings.TrimSpace(msg))
	if len(fields) == 0 {
		return Command{}
	}
	cmd := strings.ToUpper(fields[0])
	args := []string{}
	if len(fields) > 1 {
		args = fields[1:]
	}
	return Command{
		cmd:  cmd,
		args: args,
	}
}
