package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "cs3410/chord/protocol"

	"google.golang.org/grpc"
)

const (
	defaultPort       = "3410"
	successorListSize = 3
	keySize           = sha1.Size * 8
	maxLookupSteps    = 32
)

var (
	two          = big.NewInt(2)
	hashMod      = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)
	localaddress string
)

// find local IP address
func init() {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	localaddress = localAddr.IP.String()

	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	log.Printf("found local address %s\n", localaddress)
}

type Node struct {
	pb.UnimplementedChordServer
	mu          sync.RWMutex
	Address     string
	Successors  []string
	Bucket      map[string]string
	Predecessor string
	Finger      []string
}

// get the sha1 hash of a string as a bigint
func hash(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

// calculate the address of a point somewhere across the ring
// this gets the target point for a given finger table entry
// the successor of this point is the finger table entry
func jump(address string, fingerentry int) *big.Int {
	n := hash(address)

	fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
	distance := new(big.Int).Exp(two, fingerentryminus1, nil)

	sum := new(big.Int).Add(n, distance)

	return new(big.Int).Mod(sum, hashMod)
}

// returns true if elt is between start and end, accounting for the right
// if inclusive is true, it can match the end
func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}

func (n *Node) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Print("ping")
	return &pb.PingResponse{}, nil
}

func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if req.Key == "predecessor" {
		value := n.Predecessor + " " + n.Successors[0] + " " + n.Successors[1] + " " + n.Successors[2]
		return &pb.GetResponse{Value: value}, nil
	} else {
		value, exists := n.Bucket[req.Key]
		if !exists {
			return &pb.GetResponse{Value: ""}, nil
		}
		return &pb.GetResponse{Value: value}, nil
	}
}

func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Bucket[req.Key] = req.Value
	return &pb.PutResponse{}, nil
}

func (n *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.Bucket, req.Key)
	return &pb.DeleteResponse{}, nil
}

/*
	func (n *Node) GetSuccessorPredecessor(ctx context.Context, req *pb.GetSuccessorPredecessorRequest) (*pb.GetSuccessorPredecessorResponse, error) {
		n.mu.Lock()
		defer n.mu.Unlock()
		successor := n.Successors[0]

}
*/
func StartServer(port string, nprime string) (*Node, error) {
	address := net.JoinHostPort(localaddress, port)
	node := &Node{
		Address:     address,
		Successors:  nil,
		Bucket:      make(map[string]string),
		Predecessor: "",
		Finger:      make([]string, 161),
	}

	if nprime == "" {
		log.Print("StartServer: creating a new ring")
		node.Successors = []string{node.Address}
	} else {
		log.Print("StartServer: joining existing ring at ", nprime)

		// find our successor eventually....
		//... but for now use the given address as our successor
		// kick off a request to find our successor
		node.Successors = []string{nprime}
	}

	grpcServer := grpc.NewServer()

	pb.RegisterChordServer(grpcServer, node)

	listen, err := net.Listen("tcp", node.Address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Printf("Starting to listen on %v", listen.Addr())
	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	return node, nil
}

func ping(ctx context.Context, addr string) error {
	// establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	// create a new gRPC client
	client := pb.NewChordClient(conn)

	// call the ping rpc method
	_, err = client.Ping(ctx, &pb.PingRequest{})

	return err
}

func get(ctx context.Context, key string, addr string) (string, error) {
	// establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	client := pb.NewChordClient(conn)

	response, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return "", err
	}
	return response.Value, nil
}

func put(ctx context.Context, key string, value string, addr string) (string, error) {
	// establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	client := pb.NewChordClient(conn)

	_, err = client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		return "", err
	}
	return "", nil
}

func delet(ctx context.Context, key string, addr string) (string, error) {
	// establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	client := pb.NewChordClient(conn)

	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		return "", err
	}
	return "", nil
}

func stabilize(ctx context.Context, addr string) (string, error) {
	// establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	client := pb.NewChordClient(conn)

	successor_predecessor, err := client.Get(ctx, &pb.GetRequest{Key: "predecessor"}) // returns string of form "predecessor s[0] s[1] s[2]"
	if err != nil {
		return "", err
	}
	return successor_predecessor.Value, nil
}

func notify(ctx context.Context, addr string) (string, error) {
	// establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	client := pb.NewChordClient(conn)

}

func shell(node *Node, port string, joinaddress string) {
	log.Println()
	log.Print("Starting interactive shell")
	log.Print("Type \"help\" for a list of recognized commands")
	log.Println()

	ctx := context.Background()
	in := bufio.NewScanner(os.Stdin)
	//mainloop:
	for in.Scan() {
		words := strings.Fields(in.Text())
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "port":
			if len(words) == 1 {
				log.Print("port: currently set to ", port)
			} else if len(words) > 2 {
				log.Print("port: supply a port number, or omit it to report current setting")
			} else if node != nil {
				log.Print("port: cannot change port after joining a ring")
			} else {
				port = words[1]
				log.Print("port: set to ", port)
			}
		case "create":
			if len(words) > 2 {
				log.Print("create: optional port is only argument allowed")
			} else if node != nil {
				log.Print("create: already part of a ring; use quit to geto out")
			} else {
				if len(words) == 2 {
					port = words[1]
					log.Print("create: port set to ", port)
				}
				var err error
				if node, err = StartServer(port, ""); err != nil {
					log.Print("create: failed to create node: ", err)
					node = nil
				} else {
					// start background tasks each second
					go func() {
						// nextFinger := 0
						for {
							time.Sleep(time.Second / 3)
							data, err := stabilize(ctx, node.Successors[0])
							if err != nil {
								log.Print("stabilize: failed to retrieve successors predecessor information")
							}
							data_fields := strings.Fields(data)
							node.Successors[1] = data_fields[1]
							node.Successors[2] = data_fields[2]
							if data_fields[0] != node.Address {

							}
							time.Sleep(time.Second / 3)
							//nextFinger = server.fixFingers(nextFinger)
							time.Sleep(time.Second / 3)
							//server.checkPredecessor()
						}
					}()
				}
			}
		case "join":
			var err error
			if node != nil {
				log.Print("join: already part of a ring; use quit to geto out")
			} else {
				joinaddress = words[1]
				if node, err = StartServer(port, joinaddress); err != nil {
					log.Print("join: failed to create node: ", err)
					node = nil
				} else {
					node.Successors = append(node.Successors, joinaddress)
					// start background tasks each second
					go func() {
						// nextFinger := 0
						for {
							time.Sleep(time.Second / 3)
							stabilize(ctx, node.Successors[0])
							time.Sleep(time.Second / 3)
							//nextFinger = server.fixFingers(nextFinger)
							time.Sleep(time.Second / 3)
							//server.checkPredecessor()
						}
					}()
				}
			}
		case "ping":
			if len(words) != 2 {
				log.Print("ping: must supply address and nothing else")
			} else if node == nil {
				log.Print("ping: not a member of a ring")
			} else {
				addr := words[1]
				log.Print("ping: sending ping request to node ", addr)
				err := ping(ctx, addr)
				if err != nil {
					log.Print("ping: error in ping call: ", err)
				}
			}
		case "get":
			response, err := get(ctx, words[1], words[2])
			if err != nil {
				log.Print("get: error in get call: ", err)
			} else {
				log.Print(response)
			}

		case "put":
			response, err := put(ctx, words[1], words[2], words[3])
			if err != nil {
				log.Print("put: error in put call: ", err)
			} else {
				log.Print(response)
			}
		case "delete":
			response, err := delet(ctx, words[1], words[2])
			if err != nil {
				log.Print("delete: error in delete call: ", err)
			} else {
				log.Print(response)
			}
		case "dump":
			fmt.Println("Address of Node: ", node.Address)
			fmt.Println("Address of Successor: ", node.Successors[0])
			lis := fmt.Sprint(node.Bucket)
			fmt.Println(lis)
		}
	}
	if err := in.Err(); err != nil {
		log.Fatalf("scanning input: %v", err)
	}

	log.Print("quitting")

	if node == nil {
		log.Print("not part of a ring, nothing to clean up")
	}

	//send keys to successor
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	var create, doshell bool
	var port, joinaddress string
	var dumpinterval int
	var notime bool
	flag.BoolVar(&notime, "notime", notime,
		"Omit timestamp at the bginning of each output line")
	flag.StringVar(&port, "port", defaultPort,
		"Set the port for this node at address "+localaddress)
	flag.BoolVar(&create, "create", false,
		"Create a new ring")
	flag.StringVar(&joinaddress, "join", "",
		"Join an existing ring at given address (implies -create=false)")
	flag.IntVar(&dumpinterval, "dump", -1,
		"Dump status info every n seconds (<1 implies no periodic dumps)")
	flag.BoolVar(&doshell, "shell", true,
		"Start an interactive shell (implies dump=-1)")
	flag.Parse()

	if notime {
		log.SetFlags(0)
	}

	if joinaddress != "" {
		create = false
	}
	if doshell {
		dumpinterval = -1
	}

	rand.Seed(time.Now().UnixNano())

	var node *Node
	//var err error
	/*
		if create || joinaddress != "" {
			if strings.HasPrefix(joinaddress, ":") {
				joinaddress = net.JoinHostPort(localaddress, joinaddress[1:])
			} else if joinaddress != "" && !strings.Contains(joinaddress, ":") {
				joinaddress = net.JoinHostPort(joinaddress, port)
			}
			if node, err = StartServer(port, joinaddress); err != nil {
				log.Fatal("Failed creating node: ", err)
			}
		}
		if dumpinterval >= 1 {
			go func ()  {
				for {
					time.Sleep(time.Second * time.Duration(dumpinterval))
					node.dump()
				}
			}()
		}
	*/
	if doshell {
		shell(node, port, joinaddress)
	} else {
		log.Print("No interactive shell, main goroutine going to sleep")
		for {
			time.Sleep(time.Minute)
		}
	}
}
