package main

import (
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageNode struct {
	n           *maelstrom.Node
	messages    map[int]bool
	neighbors   []string
	messagesMux sync.RWMutex
}

func (s *MessageNode) addMessage(message int) bool {
	s.messagesMux.Lock()
	defer s.messagesMux.Unlock()

	if _, exists := s.messages[message]; exists {
		return true
	}

	s.messages[message] = true
	return false
}

func (s *MessageNode) getMessages() []int {
	s.messagesMux.RLock()
	defer s.messagesMux.RUnlock()

	messages := make([]int, 0, len(s.messages))
	for msg := range s.messages {
		messages = append(messages, msg)
	}
	return messages
}

func (s *MessageNode) propagateMessage(message int) {
	for _, neighbor := range s.neighbors {
		body := map[string]any{
			"type":    "broadcast",
			"message": message,
		}

		go func(dest string) {
			if err := s.n.Send(dest, body); err != nil {
				// Best Effort Delivery
				log.Printf("Error sending to %s: %v", dest, err)
			}
		}(neighbor)
	}
}
