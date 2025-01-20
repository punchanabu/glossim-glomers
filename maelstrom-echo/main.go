package main

import (
	"encoding/json"
	"log"

	maelstorm "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstorm.NewNode()

	n.Handle("echo", func(msg maelstorm.Message) error {
		// Unmarshal the message body into a map[string]any
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "echo_ok"

		// Echo the original message with the updated message type
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
