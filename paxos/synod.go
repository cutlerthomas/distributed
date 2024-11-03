package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type State struct {
	nodes    []Node
	majority int
	proposer int
	votes    int
}

func (state *State) TryInitialize(line string) bool {
	var size int
	n, err := fmt.Sscanf(line, "initialize %d nodes\n", &size)
	if err != nil || n != 1 {
		return false
	}
	state.majority = (size / 2) + 1
	for i := 0; i < size; i++ {
		state.nodes = append(state.nodes, Node{i + 1, Key{}, 0, 0, 5000 + (i + 1)})
	}
	fmt.Printf("--> initialized %d nodes\n", size)
	return true
}

func (state *State) TrySendPrepare(line string) bool {
	var node int
	var time int
	n, err := fmt.Sscanf(line, "at %d send prepare request from %d\n", &time, &node)
	if err != nil || n != 2 {
		return false
	}
	state.votes = 0
	state.proposer = node
	message := Key{MsgPrepareRequest, time, node, 0}
	for _, n := range state.nodes {
		n.message = message
	}
	fmt.Printf("--> sent prepare requests to all nodes from %d with sequence %d\n", state.proposer, state.nodes[state.proposer-1].sequence)
	return true
}

func (state *State) TryDeliverPrepareRequest(line string) bool {
	var time int
	var node int
	var time_seen int
	n, err := fmt.Sscanf(line, "at %d deliver prepare request message to %d from time %d\n", &time, &node, &time_seen)
	if err != nil || n != 3 {
		return false
	}
	for _, v := range state.nodes {
		if v.name == node {
			if v.high_time_seen < time {
				v.high_time_seen = time
				state.votes += 1
				message := Key{MsgPrepareResponse, time, v.name, 1}
				v.message = message
				fmt.Printf("--> prepare request from %d sequence %d accepted by %d with no value\n", state.proposer, state.nodes[state.proposer-1].sequence, node)
			} else {
				message := Key{MsgPrepareResponse, time, v.name, 0}
				v.message = message
				state.votes -= 1
			}
		}
	}
	return true
}

//func (state *State) TryDeliverPrepareResponse(line string) bool {

//}

type Node struct {
	name               int
	message            Key
	high_time_seen     int
	high_time_accepted int
	sequence           int
}

const (
	MsgPrepareRequest = iota
	MsgPrepareResponse
	MsgAcceptRequest
	MsgAcceptResponse
	MsgDecideRequest
)

type Key struct {
	Type   int
	Time   int
	Target int
	Ok     int
}

func MainLoop(state State) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		// trim comments
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		// ignore empty/comment-only lines
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		line += "\n"
		switch {
		case state.TryInitialize(line):
		case state.TrySendPrepare(line):
		case state.TryDeliverPrepareRequest(line):
		//case state.TryDeliverPrepareResponse(line):
		//case state.TryDeliverAcceptRequest(line):
		//case state.TryDeliverAcceptResponse(line):
		//case state.TryDeliverDecideRequest(line):
		default:
			log.Fatalf("unknown line: %s", line)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("scanner failure: %v", err)
	}
}

func main() {
	var s State
	MainLoop(s)
}
