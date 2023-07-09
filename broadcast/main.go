package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastReq struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type BroadcastRes struct {
	Type string `json:"type"`
}

type GossipReq struct {
	Type     string       `json:"type"`
	Messages map[int]bool `json:"messages"`
}

type Read struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type Topology map[string][]string

type TopologyReq struct {
	Type     string   `json:"type"`
	Topology Topology `json:"topology"`
}

type TopologyRes struct {
	Type string `json:"type"`
}

func main() {

	n := maelstrom.NewNode()

	mu := sync.Mutex{}

	messages := map[int]bool{}
	// var topology Topology

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastReq

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Thread safety is important! xoxoxox - your pc
		mu.Lock()
		messages[body.Message] = true

		for _, neighbor := range n.NodeIDs() {
			n.Send(neighbor, GossipReq{
				Type:     "gossip",
				Messages: messages,
			})
		}
		mu.Unlock()

		return n.Reply(msg, BroadcastRes{Type: "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {

		var body GossipReq

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		for msg := range body.Messages {
			messages[msg] = true
		}
		mu.Unlock()

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {

		// Thread safety is important! xoxoxox - your pc
		mu.Lock()
		defer mu.Unlock() // Unlock after we return

		return n.Reply(msg, Read{
			Type: "read_ok",
			Messages: func() []int {
				msgs := make([]int, len(messages))
				for msg := range messages {
					msgs = append(msgs, msg)
				}
				return msgs
			}(),
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {

		var body TopologyReq

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// // Thread safety is important! xoxoxox - your pc
		// mu.Lock()
		// topology = body.Topology
		// mu.Unlock()

		return n.Reply(msg, TopologyRes{
			Type: "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
