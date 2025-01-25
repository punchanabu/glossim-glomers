package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	s := &MessageNode{
		n:        maelstrom.NewNode(),
		messages: make(map[int]bool),
		pending:  make(map[int]bool),
	}

	go s.retryMessage()

	s.n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int(body["message"].(float64))

		s.addMessage(message)

		s.propagateMessage(message)

		return s.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	s.n.Handle("read", func(msg maelstrom.Message) error {
		messages := s.getMessages()
		return s.n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	s.n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology := body["topology"].(map[string]interface{})
		s.neighbors = make([]string, 0)

		neighbors := topology[s.n.ID()].([]interface{})
		for _, n := range neighbors {
			s.neighbors = append(s.neighbors, n.(string))
		}

		return s.n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	s.n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
