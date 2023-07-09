package main

import (
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Generate struct {
	Type string `json:"type"`
	Id   string `json:"id"`
}

func main() {

	n := maelstrom.NewNode()

	mu := sync.Mutex{}
	c := 0

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.

		// Thread safety is important! xoxoxox - your pc
		mu.Lock()
		uid := fmt.Sprintf("%s-%s-%d", msg.Dest, msg.Src, c)
		c += 1
		mu.Unlock()

		body := Generate{
			Type: "generate_ok",
			Id:   uid,
		}

		return n.Reply(msg, body)

	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
