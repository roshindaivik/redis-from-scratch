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
	data interface{}
	ttl *time.Time 
	typ ValueType	
}

type DB struct {
	mu sync.RWMutex
	kv map[string]Value
	conds map[string]*sync.Cond
}

type Command struct {
	cmd  string
	args []string
}

var db = DB{
	kv: make(map[string]Value),
	conds: make(map[string]*sync.Cond),
}


func RedisConnHandler(conn net.Conn) {
	defer conn.Close()
	writeRESP(conn, "Client connected: %v\r\n", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			writeRESP(conn, "Client disconnected: %v\r\n", err)
			return
		}

		command := ParseCommands(msg)
		if command.cmd == "" {
			writeRESP(conn, "-ERR empty command\r\n")
			continue
		}

		switch command.cmd {
		case "PING":
			writeRESP(conn, "+PONG\r\n")
		case "SET":
			if err := db.SetKey(command.args); err != nil {
				writeRESP(conn, "-ERR %s\r\n", err.Error())
			} else {
				writeRESP(conn, "+OK\r\n")
			}
		case "GET":
			val, ok := db.GetKey(command.args)
			if !ok {
				writeRESP(conn, "nil\r\n")
			} else {
				writeRESP(conn, "%s\r\n", val)
			}
		case "ECHO":
			if len(command.args) == 0 {
				writeRESP(conn, "-ERR wrong number of arguments for 'ECHO'\r\n")
			} else {
				writeRESP(conn, "+%s\r\n", strings.Join(command.args, " "))
			}
		case "RPUSH":
			err := db.RPush(command.args)
			if err != nil{
				writeRESP(conn, "-ERR %s\r\n", err.Error())
			}else{
				writeRESP(conn, "+OK\r\n")
			}
		case "LRANGE":
			data, err := db.LRange(command.args)
			if err != nil{
				writeRESP(conn, "-ERR %s\r\n", err.Error())
			}else{
				writeRESP(conn, "%s\r\n", data)
			}
		case "LLEN":
			data,err := db.LLen(command.args)
			if err != nil{
				writeRESP(conn, "-ERR %s\r\n", err.Error())
			}else{
				writeRESP(conn, "%v\r\n", data)
			}
		case "LPOP":
			data,err := db.LPop(command.args)
			if err != nil{
				writeRESP(conn, "-ERR %s\r\n", err.Error())
			}else{
				writeRESP(conn, "%s",data)
			}
		case "BLOP":
			data,err := db.Blop(command.args)
			if err != nil{
				writeRESP(conn, "-ERR %s\r\n", err.Error())
			}else{
				writeRESP(conn, "%s\r\n",data)
			}
		default:
			writeRESP(conn, "-ERR unknown command '%s'\r\n", command.cmd)
		}
	}
}

func (db *DB) GetKey(args []string) (string, bool) {
	if len(args) < 1 {
		return "", false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.kv[args[0]]
	if !ok {
		return "", false
	}
	if val.ttl != nil && time.Now().After(*val.ttl){
		delete(db.kv, args[0])
		return "", false
	}
	return val.data.(string), ok
}

func (db *DB) SetKey(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("wrong number of arguments for 'SET'")
	}
	key := args[0]
	var ttl *time.Time
	value := args[1]
	if len(args) > 2 && args[2] == "PX"{
		if len(args) < 4{
			return fmt.Errorf("syntax error")
		}
		ms, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil{
			return fmt.Errorf("invalid expire time in 'SET'")
		}
		expiryTime := time.Now().Add(time.Duration(ms) * time.Millisecond)
		ttl = &expiryTime	
	}	
	db.mu.Lock()
	defer db.mu.Unlock()
	db.kv[key] = Value{
		data: value,
		ttl: ttl,	
		typ: STRING,
	}
	return nil
}


// NOT HANDLING LPUSH FOR NOW 
func (db *DB) RPush(args []string) error{
	if len(args) < 2 {
		return fmt.Errorf("wrong number of arguments for 'RPUSH'")
	}
	key := args[0]
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.kv[key]
	if ok{
		if val.typ != LIST{
			return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		for _, arg := range args[1:]{			
			val.data = append(val.data.([]interface{}), arg)
		}
		db.kv[key] = val
	}else{
		data := make([]interface{}, 0, len(args)-1)
		for _, arg := range args[1:]{
			data = append(data, arg)
		}
		db.kv[key] = Value{
			data: data,
			typ: LIST,
		}
	}
	if cond,ok := db.conds[key];ok{
		cond.Broadcast()
	}
	return nil 
}

func (db *DB) Blop(args []string)(string,error){
	if len(args) < 2{
		return "",fmt.Errorf("wrong number of arguments for 'BLOP'")
	}
	key := args[0]
	timer, err := strconv.Atoi(args[1])
    if err != nil || timer < 0 {
        return "", fmt.Errorf("value is not an integer or out of range")
    }
    
    var deadline time.Time
    hasDeadline := timer > 0
    if hasDeadline {
        deadline = time.Now().Add(time.Duration(timer) * time.Second)
    }
    db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.conds[key]; !ok {
		db.conds[key] = sync.NewCond(&db.mu)
	}
	cond := db.conds[key]
	if hasDeadline{
		stopTimer := make(chan bool)
		defer close(stopTimer)
		go func(){
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for{
				select{
				case <-ticker.C:
					db.mu.Lock()
					cond.Broadcast()
					db.mu.Unlock()
				case <-stopTimer:
					return 
				}
			}
		}()
	}
	for {
		val,ok := db.kv[key]
		if ok{
			if val.typ != LIST{
				return "",fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			data := val.data.([]interface{})
			if len(data) > 0 {
				removedData := data[:1]
				val.data = data[1:]
				db.kv[key] = val
				return formRESPArray(removedData),nil
			}
		}
		if hasDeadline && time.Now().After(deadline){
			return "",fmt.Errorf("timer expired")
		}
		cond.Wait()
	}	

}

func (db * DB) LLen(args []string)(int,error){
	if len(args) < 1{
		return 0,fmt.Errorf("wrong number of arguments for 'LLEN'")
	}
	key := args[0]
	db.mu.RLock()
	defer db.mu.RUnlock()
	val,ok := db.kv[key]
	if !ok{
		return 0,nil
	}
	if val.typ != LIST{
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	data := val.data.([]interface{})
	return len(data),nil
}

func (db *DB) LPop(args []string)(string,error){
	argLen := len(args)
	if argLen < 1 || argLen >= 3{
		return "",fmt.Errorf("wrong number of arguments for 'LLEN'")
	}
	key := args[0]
	num := 1 
	if argLen == 2{
		ele, err := strconv.Atoi(args[1])
		if err != nil {
			return "", fmt.Errorf("value is not an integer or out of range")
		}
		num = ele
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.kv[key]
	if !ok {
		return "", nil
	}
	if val.typ != LIST {
		return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	data := val.data.([]interface{})
	n := len(data)
	if num < 0{
		num = n + num
	}
	if num > n{
		num = n-1
		removedData := data[:num]
		delete(db.kv, key)
		return formRESPArray(removedData),nil
	}else{
		removedData := data[:num]
		val.data = data[num:]
		db.kv[key] = val
		return formRESPArray(removedData),nil
	}
}


func (db *DB) LRange(args []string)(string, error){
	if len(args) < 3{
		return "", fmt.Errorf("wrong number of arguments for 'LRANGE'")
	}
	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return "", fmt.Errorf("value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return "", fmt.Errorf("value is not an integer or out of range")
	}
	
	db.mu.RLock()
	defer db.mu.RUnlock()	
	val, ok := db.kv[key]
	if !ok {
		return "", nil
	}
	if val.typ != LIST {
		return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	data := val.data.([]interface{})
	n := len(data)
	
	if start < 0{
		start += n 
	}
	if end < 0 {
		end += n
	}
	if start < 0 {
		start = 0
	}
	if start >= n {
		return "", nil
	}
	if end >= n{
		end = n - 1
	}
	if start > end {
		return "", nil
	}
	
	return formRESPArray(data[start:end+1]), nil
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

func formRESPArray(data []any) string{
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("Length %d\r\n", len(data)))
	for _, s := range data{
		buf.WriteString(fmt.Sprintf("%v\r\n", s))
	}
	return buf.String()
}

func writeRESP(conn net.Conn, s string, args ...any){
	if len(args) > 0 {
		fmt.Fprintf(conn, s, args...)
	} else {
		fmt.Fprint(conn, s)
	}
	fmt.Fprint(conn, "> ")
}