package main

import (
	"github.com/google/uuid"
	maelstorm "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstorm.NewNode()

	n.Handle("generate", func(msg maelstorm.Message) error {
		return n.Reply(msg, map[string]any{"type": "generate_ok", "id": uuid.New()})
	})

	n.Run()
}
