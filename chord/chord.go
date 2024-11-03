// install notes:
// go get -u google.golang.org/grpc
// go get -u google.golang.org/protobuf/proto-gen-go
// go get -u google.golang.org/protobuf/cmd/protoc-gen-go
// protoc --go_out=. --go-grpc_out=. chord.proto

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
	"strconv"
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

// The main Node data structure that holds all state for this
// node once it has joined a ring
type Node struct {
	pb.UnimplementedChordServer
	mu sync.RWMutex

	Address     string
	Predecessor string
	Successors  []string
	FingerTable []string

	Bucket map[string]string
}

//
// Helper functions
//

// find our local IP address
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

func find(id *big.Int, startNodeAddress string) (string, error) {
	var nextNodeAddress = startNodeAddress

	for i := 0; i < maxLookupSteps; i++ {
		found, successorAddress, err := find_successor_rpc(nextNodeAddress, id)
		if err != nil {
			return "", fmt.Errorf("error during find operation: %v", err)
		}
		if found {
			return successorAddress, nil
		}
		if nextNodeAddress == successorAddress {
			return "", fmt.Errorf("stuck in a loop, current node returned itself without finding id")
		}
		nextNodeAddress = successorAddress
	}

	return "", fmt.Errorf("failed to find the successor of %s after %d steps", id, maxLookupSteps)
}

// Create the node object and all of the background processors
func StartServer(port string, nprime string) (*Node, error) {
	address := net.JoinHostPort(localaddress, port)
	node := &Node{
		Address:     address,
		FingerTable: make([]string, keySize+1),
		Predecessor: "",
		Successors:  nil,
		Bucket:      make(map[string]string),
	}

	//
	// start listening for rpc calls
	//
	grpcServer := grpc.NewServer()

	// Register our service implementation with the gRPC server
	pb.RegisterChordServer(grpcServer, node)

	// bind to the port
	lis, err := net.Listen("tcp", node.Address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Start the gRPC server
	log.Printf("Starting to listen on %v", lis.Addr())
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// are we the first node?
	if nprime == "" {
		log.Print("StartServer: creating new ring")
		node.Successors = []string{node.Address}
		go node.startStabilization()
	} else {
		log.Print("StartServer: joining existing ring using ", nprime)

		id := hash(node.Address)
		successor, err := find(id, nprime)
		if err != nil {
			return nil, err
		}
		node.Successors = []string{successor}

		// Fetch and transfer keys from successor
		err = node.fetchAndStoreKeys()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch and store keys from successor: %v", err)
		}

		// start running background tasks every second or so
		go node.startStabilization()

	}

	return node, nil
}

func (node *Node) fetchAndStoreKeys() error {
	successor := node.Successors[0]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transferredKeys, err := get_all_rpc(ctx, node.Address, successor)
	if err != nil {
		return err
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	for key, value := range transferredKeys {
		node.Bucket[key] = value
	}
	log.Printf("Transferred %d keys from successor %s to current node", len(transferredKeys), successor)
	return nil
}

func (node *Node) startStabilization() {
	nextFinger := 0
	for {
		time.Sleep(time.Second / 3)
		node.stabilize()

		time.Sleep(time.Second / 3)
		nextFinger = node.fixFingers(nextFinger)

		time.Sleep(time.Second / 3)
		node.checkPredecessor()
	}
}

//
// All the methods that are called via RPC
//

func (n *Node) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{}, nil
}

func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	keyID := hash(req.Key)
	// Find the node that should have this key
	successor, err := find(keyID, n.Address)
	if err != nil {
		return nil, err
	}

	// If the current node is the successor, handle the request
	if successor == n.Address {
		n.mu.RLock()
		defer n.mu.RUnlock()
		value, exists := n.Bucket[req.Key]
		if !exists {
			log.Printf("Get: Key [%s] not found", req.Key)
			return &pb.GetResponse{Value: ""}, nil
		}
		log.Printf("Get: Key [%s] found with value [%s]", req.Key, value)
		return &pb.GetResponse{Value: value}, nil
	}

	// Otherwise, forward the request to the successor
	return get_rpc(ctx, req.Key, successor)
}

func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	keyID := hash(req.Key)
	successor, err := find(keyID, n.Address)
	if err != nil {
		return nil, err
	}

	if successor == n.Address {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.Bucket[req.Key] = req.Value
		log.Printf("Put: Key [%s] set to [%s]", req.Key, req.Value)
		return &pb.PutResponse{}, nil
	}

	return put_rpc(ctx, req.Key, req.Value, successor)
}

func (n *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	keyID := hash(req.Key)
	successor, err := find(keyID, n.Address)
	if err != nil {
		return nil, err
	}

	if successor == n.Address {
		n.mu.Lock()
		defer n.mu.Unlock()
		if _, exists := n.Bucket[req.Key]; exists {
			delete(n.Bucket, req.Key)
			log.Printf("Delete: Key [%s] removed", req.Key)
		}
		return &pb.DeleteResponse{}, nil
	}

	return delete_rpc(ctx, req.Key, successor)
}

func (n *Node) PutAll(ctx context.Context, req *pb.PutAllRequest) (*pb.PutAllResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for key, value := range req.Pairs {
		n.Bucket[key] = value
		log.Printf("PutAll: Key [%s] set to [%s]", key, value)
	}

	return &pb.PutAllResponse{Success: true}, nil
}

func (n *Node) GetAll(ctx context.Context, req *pb.GetAllRequest) (*pb.GetAllResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	new_node_id := hash(req.NewNodeAddress)
	predecessor_id := hash(n.Predecessor)

	result := make(map[string]string)
	for key, value := range n.Bucket {
		key_id := hash(key)
		// Check if the key belongs to the new node
		if between(predecessor_id, key_id, new_node_id, false) {
			result[key] = value
			delete(n.Bucket, key)
			log.Printf("GetAll: Key [%s] moved to new node [%s]", key, req.NewNodeAddress)
		}
	}

	return &pb.GetAllResponse{Pairs: result}, nil
}

func (n *Node) FindSuccessor(ctx context.Context, req *pb.FindSuccessorRequest) (*pb.FindSuccessorResponse, error) {
	id := new(big.Int).SetBytes(req.Id)
	n.mu.RLock()
	currentID := hash(n.Address)
	successorID := hash(n.Successors[0])
	n.mu.RUnlock()

	// If id is between current node and its successor, return successor
	if between(currentID, id, successorID, true) {
		return &pb.FindSuccessorResponse{
			Found:     true,
			Successor: n.Successors[0],
		}, nil
	}

	// Use the closest preceding node from the finger table
	closest := n.closest_preceding_node(id)
	if closest == n.Address {
		// If the closest node is itself and not found, it means we need to forward to successor
		return &pb.FindSuccessorResponse{
			Found:     false,
			Successor: n.Successors[0],
		}, nil
	}

	// Otherwise, make a recursive call to find the successor of id starting from the closest preceding node
	found, successorAddress, err := find_successor_rpc(closest, id)
	if err != nil {
		return nil, err
	}
	return &pb.FindSuccessorResponse{
		Found:     found,
		Successor: successorAddress,
	}, nil
}

func (n *Node) GetPredecessorAndSuccessors(ctx context.Context, req *pb.GetPredecessorAndSuccessorsRequest) (*pb.GetPredecessorAndSuccessorsResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &pb.GetPredecessorAndSuccessorsResponse{
		Predecessor: n.Predecessor,
		Successors:  n.Successors,
	}, nil
}

func (n *Node) Notify(ctx context.Context, req *pb.NotifyRequest) (*pb.NotifyResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Predecessor == "" || between(hash(n.Predecessor), hash(req.CallerAddress), hash(n.Address), false) {
		log.Println("new predecessor found from notify")
		n.Predecessor = req.CallerAddress
	}
	return &pb.NotifyResponse{}, nil
}

//
// Methods that are only used locally
//

func (n *Node) closest_preceding_node(id *big.Int) string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Check finger table in reverse order for the closest preceding node
	for i := len(n.FingerTable) - 1; i >= 0; i-- {
		fingerID := hash(n.FingerTable[i])
		if between(hash(n.Address), fingerID, id, false) {
			return n.FingerTable[i]
		}
	}

	// Fallback to the successor if no suitable node is found in the finger table
	return n.Successors[0]
}

func (n *Node) checkPredecessor() {
	n.mu.Lock()
	predecessor := n.Predecessor
	n.mu.Unlock()
	err := ping_rpc(context.Background(), predecessor)
	if predecessor != "" && err != nil {
		n.mu.Lock()
		n.Predecessor = ""
		n.mu.Unlock()
	}
}

func (n *Node) stabilize() {
	n.mu.Lock()
	if len(n.Successors) == 0 {
		n.mu.Unlock()
		return
	}

	successor := n.Successors[0]
	ctx := context.Background()
	n.mu.Unlock()

	// Get predecessor of the successor
	resp, err := get_predecessor_and_successors_rpc(ctx, successor)
	if err != nil {
		log.Printf("stabilize: Unable to contact successor %s: %v", successor, err)
		n.handleSuccessorFailure()
		return
	}

	n.mu.Lock()
	predecessorOfSuccessor := resp.Predecessor
	if predecessorOfSuccessor != "" && between(hash(n.Address), hash(predecessorOfSuccessor), hash(successor), false) {
		successor = predecessorOfSuccessor
	}
	n.Successors[0] = successor // always update successor to the closest known successor
	n.mu.Unlock()

	notify_rpc(ctx, n.Address, successor)
}

func (n *Node) handleSuccessorFailure() {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Println("updating successor list")
	n.Successors = n.Successors[1:]
	if len(n.Successors) == 0 {
		n.Successors = []string{n.Address} // Fallback to self if no successors left
	}
}

func (n *Node) fixFingers(currentIndex int) int {
	n.mu.Lock()
	if currentIndex > keySize {
		currentIndex = 1
	}

	// Calculate the start value for the current finger
	start := jump(n.Address, currentIndex)
	n.mu.Unlock()

	successor, err := find(start, n.Address)
	if err != nil {
		log.Printf("Error finding successor for finger %d: %v", currentIndex, err)
		return currentIndex // If error occurs, retry the same index next time
	}

	n.mu.Lock()
	// Update the current finger table entry
	n.FingerTable[currentIndex] = successor

	// Check and update subsequent fingers that may also be affected
	nextIndex := currentIndex + 1
	for ; nextIndex <= keySize; nextIndex++ {
		nextStart := jump(n.Address, nextIndex)
		if !between(hash(n.Address), nextStart, hash(successor), true) {
			break // Stop if the next start is outside the current successor's range
		}
		n.FingerTable[nextIndex] = successor
	}
	n.mu.Unlock()

	return nextIndex % (keySize + 1) // Wrap around the index after the last finger table entry
}

//
// "client" stuff, starting with the shell
//

func shell(node *Node, port string, joinaddress string) {
	log.Println()
	log.Print("Starting interactive shell")
	log.Print("Type \"help\" for a list of recognized commands")
	log.Println()

	ctx := context.Background()
	in := bufio.NewScanner(os.Stdin)
mainloop:
	for in.Scan() {
		// get a line of input
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
				log.Print("create: already part of a ring; use quit to get out")
			} else {
				if len(words) == 2 {
					port = words[1]
					log.Print("create: port set to ", port)
				}
				var err error
				if node, err = StartServer(port, ""); err != nil {
					log.Print("create: failed to create node: ", err)
					node = nil
				}
			}
		case "join":
			if len(words) > 2 {
				log.Print("join: join address is the only argument expected")
			} else if joinaddress == "" && len(words) == 1 {
				log.Print("join: must supply address of existing ring")
			} else if node != nil {
				log.Print("join: already part of a ring; use quit to get out")
			} else {
				if len(words) == 2 {
					joinaddress = words[1]
				}
				if strings.HasPrefix(joinaddress, ":") {
					joinaddress = net.JoinHostPort(localaddress, joinaddress[1:])
				} else if !strings.Contains(joinaddress, ":") {
					joinaddress = net.JoinHostPort(joinaddress, port)
				}
				var err error
				if node, err = StartServer(port, joinaddress); err != nil {
					log.Print("join: failed to join node: ", err)
					node = nil
				}
			}
		case "dump":
			if len(words) > 1 {
				log.Print("dump: no arguments expected")
			} else if node == nil {
				log.Print("dump: not a member of a ring")
			} else {
				node.dump()
			}
		case "put":
			if len(words) != 3 {
				log.Print("put: requires key and value")
				return
			}
			key, value := words[1], words[2]
			_, err := put_rpc(ctx, key, value, node.Address) // Assume node.Address resolves to current node for self-calls
			if err != nil {
				log.Printf("Error putting key: %v", err)
			} else {
				log.Printf("Key [%s] set successfully.", key)
			}
		case "putrandom":
			if len(words) != 2 {
				log.Print("putrandom: must supply a number of random keys to put")
			} else if node == nil {
				log.Print("putrandom: not a member of a ring")
			} else {
				n, err := strconv.Atoi(words[1])
				if err != nil {
					log.Print("putrandom: failed to parse number: ", err)
					continue
				}
				for i := 0; i < n; i++ {
					// generate a random key of 5 letters
					key := ""
					for j := 0; j < 5; j++ {
						key += fmt.Sprintf("%c", 'a'+rand.Intn(26))
					}
					value := "random(" + key + ")"
					log.Print("putrandom: generated pair " + key + " => " + value)

					_, err := put_rpc(ctx, key, value, node.Address) // Assume node.Address resolves to current node for self-calls
					if err != nil {
						log.Printf("Error putting key: %v", err)
					} else {
						log.Printf("Key [%s] set successfully.", key)
					}
				}
			}

		case "get":
			if len(words) != 2 {
				log.Print("get: requires a key")
				return
			}
			key := words[1]
			response, err := get_rpc(ctx, key, node.Address) // Node lookup handled internally
			if err != nil {
				log.Printf("Error getting key: %v", err)
			} else if response.Value == "" {
				log.Printf("Key [%s] not found.", key)
			} else {
				log.Printf("Value for key [%s]: %s", key, response.Value)
			}
		case "delete":
			if len(words) != 2 {
				log.Print("delete: requires a key")
				return
			}
			key := words[1]
			_, err := delete_rpc(ctx, key, node.Address) // Node lookup handled internally
			if err != nil {
				log.Printf("Error deleting key: %v", err)
			} else {
				log.Printf("Key [%s] deleted successfully.", key)
			}
		case "quit":
			if node != nil {
				// Ensure there is a successor to receive the data
				if len(node.Successors) > 0 && node.Successors[0] != node.Address {
					// Send all key/value pairs to the successor
					if _, err := put_all_rpc(ctx, node.Bucket, node.Successors[0]); err != nil {
						log.Printf("Failed to transfer data to successor: %v", err)
					} else {
						log.Println("Data transferred to successor successfully.")
					}
				}
				log.Print("Shutting down node.")
			}
			break mainloop
		case "help":
			fallthrough
		default:
			log.Print("commands: help, quit, port, create, join, dump, put, get, delete")
		}
	}
	if err := in.Err(); err != nil {
		log.Fatalf("scanning input: %v", err)
	}

	log.Print("quitting")

	if node == nil {
		log.Print("not part of a ring, nothing to clean up")
		return
	}
}

// format an address for printing
func addr(a string) string {
	if a == "" {
		return "(empty)"
	}
	s := fmt.Sprintf("%040x", hash(a))
	return s[:8] + ".. (" + a + ")"
}

// print useful info about the local node
func (n *Node) dump() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	log.Println()
	log.Print("dump: information about this node")

	// predecessor and successor links
	log.Print("Neighborhood")
	log.Print("pred:   ", addr(n.Predecessor))
	log.Print("self:   ", addr(n.Address))
	for i, succ := range n.Successors {
		log.Printf("succ %d: %s", i, addr(succ))
	}
	log.Println()
	log.Print("Finger table")
	i := 1
	for i <= keySize {
		for i < keySize && n.FingerTable[i] == n.FingerTable[i+1] {
			i++
		}
		log.Printf(" [%3d]: %s", i, addr(n.FingerTable[i]))
		i++
	}
	log.Println()
	log.Print("Data items")
	for k, v := range n.Bucket {
		s := fmt.Sprintf("%040x", hash(k))
		log.Printf("    %s.. %s => %s", s[:8], string(k), v)
	}
	log.Println()
}

//
// Client RPC helpers
//

func ping_rpc(ctx context.Context, addr string) error {
	// Establish a new gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create a new gRPC client
	client := pb.NewChordClient(conn)

	// Call the Ping RPC method
	_, err = client.Ping(ctx, &pb.PingRequest{})
	return err
}

func put_rpc(ctx context.Context, key, value, addr string) (*pb.PutResponse, error) {
	// Establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect for Put RPC: %v", err)
		return nil, err
	}
	defer conn.Close()

	// Create a new Chord client from the connection
	client := pb.NewChordClient(conn)

	// Prepare and send the Put request
	req := &pb.PutRequest{Key: key, Value: value}
	resp, err := client.Put(ctx, req)
	if err != nil {
		log.Printf("Put RPC failed: %v", err)
		return nil, err
	}
	return resp, nil
}

func get_rpc(ctx context.Context, key string, addr string) (*pb.GetResponse, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewChordClient(conn)
	response, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func delete_rpc(ctx context.Context, key, addr string) (*pb.DeleteResponse, error) {
	// Establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect for Delete RPC: %v", err)
		return nil, err
	}
	defer conn.Close()

	// Create a new Chord client from the connection
	client := pb.NewChordClient(conn)

	// Prepare and send the Delete request
	req := &pb.DeleteRequest{Key: key}
	resp, err := client.Delete(ctx, req)
	if err != nil {
		log.Printf("Delete RPC failed: %v", err)
		return nil, err
	}
	return resp, nil
}

func get_all_rpc(ctx context.Context, newNodeAddress string, addr string) (map[string]string, error) {
	// Establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect for GetAll RPC: %v", err)
		return nil, err
	}
	defer conn.Close()

	// Create a new Chord client from the connection
	client := pb.NewChordClient(conn)

	// Prepare and send the GetAll request
	req := &pb.GetAllRequest{NewNodeAddress: newNodeAddress}
	resp, err := client.GetAll(ctx, req)
	if err != nil {
		log.Printf("GetAll RPC failed: %v", err)
		return nil, err
	}
	return resp.Pairs, nil
}

func put_all_rpc(ctx context.Context, pairs map[string]string, addr string) (*pb.PutAllResponse, error) {
	// Establish a gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect for PutAll RPC: %v", err)
		return nil, err
	}
	defer conn.Close()

	// Create a new Chord client from the connection
	client := pb.NewChordClient(conn)

	// Prepare and send the PutAll request
	req := &pb.PutAllRequest{Pairs: pairs}
	resp, err := client.PutAll(ctx, req)
	if err != nil {
		log.Printf("PutAll RPC failed: %v", err)
		return nil, err
	}
	return resp, nil
}

func find_successor_rpc(nodeAddress string, id *big.Int) (bool, string, error) {
	conn, err := grpc.Dial(nodeAddress, grpc.WithInsecure())
	if err != nil {
		return false, "", fmt.Errorf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewChordClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &pb.FindSuccessorRequest{
		Id: id.Bytes(),
	}
	resp, err := client.FindSuccessor(ctx, req)
	if err != nil {
		return false, "", fmt.Errorf("failed to find successor: %v", err)
	}

	return resp.Found, resp.Successor, nil
}

func get_predecessor_and_successors_rpc(ctx context.Context, nodeAddress string) (*pb.GetPredecessorAndSuccessorsResponse, error) {
	// Establish a gRPC connection to the node
	conn, err := grpc.Dial(nodeAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("get_predecessor_and_successors_rpc: failed to connect to %s: %v", nodeAddress, err)
		return nil, err
	}
	defer conn.Close()

	// Create a Chord client from the connection
	client := pb.NewChordClient(conn)

	// Send a GetPredecessorAndSuccessorsRequest to the node
	response, err := client.GetPredecessorAndSuccessors(ctx, &pb.GetPredecessorAndSuccessorsRequest{})
	if err != nil {
		log.Printf("get_predecessor_and_successors_rpc: failed to get data from %s: %v", nodeAddress, err)
		return nil, err
	}

	return response, nil
}

func notify_rpc(ctx context.Context, callerAddress, successorAddress string) error {
	// Establish a gRPC connection to the successor
	conn, err := grpc.Dial(successorAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("notify_rpc: failed to connect to %s: %v", successorAddress, err)
		return err
	}
	defer conn.Close()

	// Create a Chord client from the connection
	client := pb.NewChordClient(conn)

	// Send a NotifyRequest to the successor
	_, err = client.Notify(ctx, &pb.NotifyRequest{CallerAddress: callerAddress})
	if err != nil {
		log.Printf("notify_rpc: failed to notify %s: %v", successorAddress, err)
		return err
	}

	return nil
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	var create, doshell bool
	var port, joinaddress string
	var dumpinterval int
	var notime bool
	flag.BoolVar(&notime, "notime", notime,
		"Omit timestamp at the beginning of each output line")
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

	// enforce implied options
	if joinaddress != "" {
		create = false
	}
	if doshell {
		dumpinterval = -1
	}

	rand.Seed(time.Now().UnixNano())

	log.Print("CS 3410: Distributed Systems")
	log.Print("Chord Distributed Hash Table (DHT) Example Solution")
	log.Print("Starter code by Russ Ross, March 2024")
	log.Print("Finished by Cutler Thomas, April 2024")
	log.Println()

	var node *Node
	var err error
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
		go func() {
			for {
				time.Sleep(time.Second * time.Duration(dumpinterval))
				node.dump()
			}
		}()
	}

	if doshell {
		shell(node, port, joinaddress)
	} else {
		log.Print("No interactive shell, main goroutine going to sleep")
		for {
			time.Sleep(time.Minute)
		}
	}
}
