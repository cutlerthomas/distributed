package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	MsgRegister = iota
	MsgList
	MsgCheckMessages
	MsgTell
	MsgSay
	MsgQuit
	MsgShutdown
)

var mutex sync.Mutex
var messages map[string][]string
var shutdown chan struct{}

func server(listenAddress string) {
	shutdown = make(chan struct{})
	messages = make(map[string][]string)

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go dispatch(conn)
	}

	<-shutdown
	time.Sleep(100 * time.Millisecond)
}

func dispatch(conn net.Conn) {
	// handle a single incomming request:
	request_len := make([]byte, 2) // 1. Read the length (uint16)
	_, err := conn.Read(request_len)
	if err != nil {
		log.Println("error reading request length: ", err)
	}
	data_len := binary.BigEndian.Uint16(request_len)
	request := make([]byte, data_len) // 2. Read the entire message into a []byte
	n, err := conn.Read(request)
	if err != nil {
		log.Println("error reading request: ", err)
	}
	request_body := request[2:]
	t := request[:2] // 3. From the message, parse the message type (uint16)
	type_int, _, err := ReadUint16(t)
	if type_int != MsgCheckMessages {
		fmt.Println("number of bytes recieved: " + fmt.Sprint(n))
		fmt.Println("bytes received: " + string(request))
		fmt.Println(request)
	}
	var response []byte
	var length []byte
	switch type_int { // 4. Call the appropriate server stub, giving it the
	//    remainder of the request []byte and collecting
	//    the response []byte
	case MsgTell:
		response = TellServerStub(request_body)
	case MsgSay:
		response = SayServerStub(request_body)
	case MsgList:
		response = ListServerStub(request_body)
	case MsgQuit:
		response = QuitServerStub(request_body)
	case MsgShutdown:
		response = ShutdownServerStub(request_body)
	case MsgRegister:
		response = RegisterServerStub(request_body)
	case MsgCheckMessages:
		response = CheckMessagesServerStub(request_body)
	}
	length = WriteUint16(length, uint16(len(response)))
	conn.Write(length)
	conn.Write(response)
	// 5. Write the message length (uint16)
	// 6. Write the message []byte
	conn.Close()
	// 7. Close the connection
	//
	// On any error, be sure to close the connection, log a
	// message, and return (a request error should not kill
	// the entire server)
}

func client(serverAddress, username string) {
	err := RegisterRPC(serverAddress, username)
	if err != nil {
		log.Println("register client stub error:", err)
	}
	go func() {
		for {
			var queue []string
			queue, err := CheckMessagesRPC(serverAddress, username)
			if err != nil {
				log.Println("checkmessages client stub error:", err)
				continue
			}
			for _, msg := range queue {
				fmt.Println(msg)
			}
			time.Sleep(1 * time.Second)
		}
	}()
	for {
		input := bufio.NewScanner(os.Stdin)
		input.Scan()
		text := input.Text()
		text_fields := strings.Fields(text)
		switch text_fields[0] {

		case "say":
			msg := strings.Join(text_fields[1:], " ")
			err := SayRPC(serverAddress, username, msg)
			if err != nil {
				log.Fatal("say client stub error:", err)
			}
		case "tell":
			target := text_fields[1]
			msg := strings.Join(text_fields[2:], " ")
			err := TellRPC(serverAddress, username, target, msg)
			if err != nil {
				log.Fatal("tell client stub error:", err)
			}
		case "quit":
			err := QuitRPC(serverAddress, username)
			if err != nil {
				log.Fatal("quit client stub error:", err)
			}
		case "help":
			fmt.Println("available commands:")
			fmt.Println("say <message>: sends some <message> to all users")
			fmt.Println("tell <user> <message>: sends some <message> to <user>")
			fmt.Println("list: prints a list of all users currently logged in")
			fmt.Println("quit: logs user out and closes client")
			fmt.Println("shutdown: shuts down the server")
		case "shutdown":
			err := ShutdownRPC(serverAddress)
			if err != nil {
				log.Fatal("shutdown client stub error:", err)
			}
		case "list":
			users, err := ListRPC(serverAddress)
			if err != nil {
				log.Fatal("list client stub error:", err)
			}
			for _, user := range users {
				fmt.Println(user)
			}
		case "":

		default:
			fmt.Println("unrecognized command, available commands:")
			fmt.Println("say <message>: sends some <message> to all users")
			fmt.Println("tell <user> <message>: sends some <message> to <user>")
			fmt.Println("list: prints a list of all users currently logged in")
			fmt.Println("quit: logs user out and closes client")
			fmt.Println("shutdown: shuts down the server")
			fmt.Println("help: prints a list of available commands")
		}
	}
}

func Register(user string) error {
	if len(user) < 1 || len(user) > 20 {
		return fmt.Errorf("Register: user must be between 1 and 20 letters")
	}
	for _, r := range user {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return fmt.Errorf("Register: user must only contain letters and digits")
		}
	}
	mutex.Lock()
	defer mutex.Unlock()

	msg := fmt.Sprintf("*** %s has logged in", user)
	log.Printf(msg)
	for target, queue := range messages {
		messages[target] = append(queue, msg)
	}
	messages[user] = nil

	return nil
}

func RegisterRPC(server, user string) error {
	client, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()

	var b []byte
	var l []byte
	b = WriteUint16(b, MsgRegister)
	b = WriteString(b, user)
	l = WriteUint16(l, uint16(len(b)))
	_, err = client.Write(l)
	_, err = client.Write(b)

	buf := make([]byte, 2)
	_, err = client.Read(buf)
	data_len := binary.BigEndian.Uint16(buf)
	buf = make([]byte, data_len)
	_, err = client.Read(buf)
	return err
}

func RegisterServerStub(request []byte) []byte {
	msg, _, err := ReadString(request)
	if err != nil {
		log.Println("error in register server stub:", err)
	}
	user := msg
	err = Register(user)
	if err != nil {
		log.Println("error in register func:", err)
	}
	var b []byte
	if err == nil {
		b = WriteUint16(b, 0)
	} else {
		b = WriteUint16(b, uint16(len(err.Error())))
		b = WriteString(b, err.Error())
	}
	return b
}

func List() []string {
	mutex.Lock()
	defer mutex.Unlock()

	var users []string
	for target := range messages {
		users = append(users, target)
	}
	sort.Strings(users)

	return users
}

func ListRPC(server string) ([]string, error) {
	client, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()

	var b []byte
	var l []byte
	b = WriteUint16(b, MsgList)
	l = WriteUint16(l, uint16(len(b)))
	_, err = client.Write(l)
	_, err = client.Write(b)

	buf := make([]byte, 2)
	_, err = client.Read(buf)
	data_len := binary.BigEndian.Uint16(buf)
	buf = make([]byte, data_len)
	_, err = client.Read(buf)
	list, _, err := ReadStringSlice(buf)
	return list, err
}

func ListServerStub(request []byte) []byte {
	users := List()
	var users_bytes []byte
	users_bytes = WriteStringSlice(users_bytes, users)
	return users_bytes
}

func CheckMessages(user string) []string {
	mutex.Lock()
	defer mutex.Unlock()

	if queue, present := messages[user]; present {
		messages[user] = nil
		return queue
	} else {
		return []string{"*** You are not logged in, " + user}
	}
}

func CheckMessagesRPC(server, user string) ([]string, error) {
	client, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()

	var b []byte
	var l []byte
	b = WriteUint16(b, MsgCheckMessages)
	b = WriteString(b, user)
	l = WriteUint16(l, uint16(len(b)))
	_, err = client.Write(l)
	_, err = client.Write(b)

	buf := make([]byte, 2)
	_, err = client.Read(buf)
	data_len := binary.BigEndian.Uint16(buf)
	buf = make([]byte, data_len)
	_, err = client.Read(buf)
	list, _, err := ReadStringSlice(buf)
	return list, err
}

func CheckMessagesServerStub(request []byte) []byte {
	user, _, err := ReadString(request)
	msgs := CheckMessages(user)
	var msgs_bytes []byte
	msgs_bytes = WriteStringSlice(msgs_bytes, msgs)
	var b []byte
	b = append(b, msgs_bytes...)
	e := make([]byte, 2)
	if err == nil {
		e = WriteUint16(e, uint16(0))
	} else {
		e = WriteUint16(e, uint16(len(err.Error())))
	}
	b = append(b, e...)
	return b
}

func Tell(user, target, message string) {
	mutex.Lock()
	defer mutex.Unlock()

	msg := fmt.Sprintf("%s tells you %s", user, message)
	if queue, present := messages[target]; present {
		messages[target] = append(queue, msg)
	} else if queue, present := messages[user]; present {
		messages[user] = append(queue, "*** No such user: "+target)
	}
}

func TellRPC(server, user, target, message string) error {
	client, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()

	var b []byte
	var l []byte
	b = WriteUint16(b, MsgTell)
	b = WriteString(b, user)
	b = WriteString(b, target)
	b = WriteString(b, message)
	l = WriteUint16(l, uint16(len(b)))
	_, err = client.Write(l)
	_, err = client.Write(b)

	buf := make([]byte, 2)
	_, err = client.Read(buf)
	data_len := binary.BigEndian.Uint16(buf)
	buf = make([]byte, data_len)
	_, err = client.Read(buf)
	return err
}

func TellServerStub(request []byte) []byte {
	var b []byte
	user, request, err := ReadString(request)
	target, request, err := ReadString(request)
	msg, request, err := ReadString(request)
	Tell(user, target, msg)
	if err == nil {
		b = WriteUint16(b, 0)
	} else {
		b = WriteUint16(b, uint16(len(err.Error())))
	}
	return b
}

func Say(user, message string) {
	mutex.Lock()
	defer mutex.Unlock()

	msg := fmt.Sprintf("%s says %s", user, message)
	for target, queue := range messages {
		messages[target] = append(queue, msg)
	}
}

func SayRPC(server, user, message string) error {
	client, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()

	var b []byte
	var l []byte
	b = WriteUint16(b, MsgSay)
	b = WriteString(b, user)
	b = WriteString(b, message)
	l = WriteUint16(l, uint16(len(b)))
	_, err = client.Write(l)
	_, err = client.Write(b)

	buf := make([]byte, 2)
	_, err = client.Read(buf)
	data_len := binary.BigEndian.Uint16(buf)
	buf = make([]byte, data_len)
	_, err = client.Read(buf)
	return err
}

func SayServerStub(request []byte) []byte {
	var b []byte
	user, request, err := ReadString(request)
	msg, request, err := ReadString(request)
	Say(user, msg)
	if err == nil {
		b = WriteUint16(b, 0)
	} else {
		b = WriteUint16(b, uint16(len(err.Error())))
	}
	return b
}

func Quit(user string) {
	mutex.Lock()
	defer mutex.Unlock()

	msg := fmt.Sprintf("*** %s has logged out", user)
	log.Print(msg)
	for target, queue := range messages {
		messages[target] = append(queue, msg)
	}
	delete(messages, user)
}

func QuitRPC(server, user string) error {
	client, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()

	var b []byte
	var l []byte
	b = WriteUint16(b, MsgQuit)
	b = WriteString(b, user)
	l = WriteUint16(l, uint16(len(b)))
	_, err = client.Write(l)
	_, err = client.Write(b)

	buf := make([]byte, 2)
	_, err = client.Read(buf)
	data_len := binary.BigEndian.Uint16(buf)
	buf = make([]byte, data_len)
	_, err = client.Read(buf)
	return err
}

func QuitServerStub(request []byte) []byte {
	user, _, err := ReadString(request)
	Quit(user)
	var b []byte
	if err == nil {
		b = WriteUint16(b, 0)
	} else {
		b = WriteUint16(b, uint16(len(err.Error())))
	}
	return b
}

func Shutdown() {
	shutdown <- struct{}{}
}

func ShutdownRPC(server string) error {
	client, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer client.Close()

	var b []byte
	var l []byte
	b = WriteUint16(b, MsgShutdown)
	l = WriteUint16(l, uint16(len(b)))
	_, err = client.Write(l)
	_, err = client.Write(b)

	buf := make([]byte, 2)
	_, err = client.Read(buf)
	data_len := binary.BigEndian.Uint16(buf)
	buf = make([]byte, data_len)
	_, err = client.Read(buf)
	return err
}

func ShutdownServerStub(request []byte) []byte {
	Shutdown()
	var b []byte
	b = WriteUint16(b, 0)
	return b
}

// WriteUint16 writes a 16-bit value as two bytes in network byte order to a []byte
func WriteUint16(buf []byte, n uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, n)
	return append(buf, b...)
}

// ReadUint16 reads a 16-bit value from a []byte as two bytes in network byte order
func ReadUint16(buf []byte) (uint16, []byte, error) {
	if len(buf) < 2 {
		return 0, nil, errors.New("input too short")
	}
	return binary.BigEndian.Uint16(buf[:2]), buf[2:], nil
}

// WriteString writes a string to a []byte
func WriteString(buf []byte, s string) []byte {
	buf = WriteUint16(buf, uint16(len(s)))
	buf = append(buf, []byte(s)...)
	return buf
}

// ReadString reads a string from a []byte
func ReadString(buf []byte) (string, []byte, error) {
	length, buf, err := ReadUint16(buf)
	if err != nil {
		return "", nil, err
	}
	if len(buf) < int(length) {
		return "", nil, errors.New("input too short")
	}
	str := string(buf[:length])
	return str, buf[length:], nil
}

// WriteStringSlice writes a slice of strings
func WriteStringSlice(buf []byte, s []string) []byte {
	buf = WriteUint16(buf, uint16(len(s)))
	for _, str := range s {
		buf = WriteString(buf, str)
	}
	return buf
}

// ReadStringSlice reads a slice of strings
func ReadStringSlice(buf []byte) ([]string, []byte, error) {
	count, buf, err := ReadUint16(buf)
	if err != nil {
		return nil, nil, err
	}
	var slice []string
	for i := uint16(0); i < count; i++ {
		var str string
		str, buf, err = ReadString(buf)
		if err != nil {
			return nil, nil, err
		}
		slice = append(slice, str)
	}
	return slice, buf, nil
}

func main() {
	log.SetFlags(log.Ltime)

	var listenAddress string
	var serverAddress string
	var username string

	switch len(os.Args) {
	case 2:
		listenAddress = net.JoinHostPort("", os.Args[1])
	case 3:
		serverAddress = os.Args[1]
		if strings.HasPrefix(serverAddress, ":") {
			serverAddress = "localhost" + serverAddress
		}
		username = strings.TrimSpace(os.Args[2])
		if username == "" {
			log.Fatal("empty user name")
		}
	default:
		log.Fatalf("Usage: %s <port>   OR   %s <server> <user>",
			os.Args[0], os.Args[0])
	}

	if len(listenAddress) > 0 {
		server(listenAddress)
	} else {
		client(serverAddress, username)
	}
}
