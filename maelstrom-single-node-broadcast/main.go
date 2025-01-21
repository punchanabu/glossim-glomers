package main

import (
	"encoding/json"

	maelstorm "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	values := []int{}

	n := maelstorm.NewNode()

	n.Handle("broadcast", func(msg maelstorm.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int(body["message"].(float64))
		values = append(values, message)

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstorm.Message) error {
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": values})
	})

	n.Handle("topology", func(msg maelstorm.Message) error {
		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	n.Run()
}
