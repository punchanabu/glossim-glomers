package main

import (
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageNode struct {
	n           *maelstrom.Node
	messages    map[int]bool
	neighbors   []string
	pending     map[int]bool
	messagesMux sync.RWMutex
	pendingMux  sync.RWMutex
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

func (s *MessageNode) retryMessage() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		s.pendingMux.Lock()
		pending := make([]int, 0, len(s.pending))
		for msg := range s.pending {
			pending = append(pending, msg)
		}
		s.pendingMux.Unlock()

		for _, msg := range pending {
			s.messagesMux.Lock()
			s.messages[msg] = true
			s.messagesMux.Unlock()

			allSent := true
			for _, neighbor := range s.neighbors {
				success := false
				for i := 0; i < 5; i++ {
					body := map[string]any{
						"type":    "broadcast",
						"message": msg,
					}
					if err := s.n.Send(neighbor, body); err == nil {
						success = true
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				if !success {
					allSent = false
				}
			}

			if allSent {
				s.pendingMux.Lock()
				delete(s.pending, msg)
				s.pendingMux.Unlock()
			}
		}
	}
}

func (s *MessageNode) propagateMessage(message int) {
	s.messagesMux.Lock()
	s.messages[message] = true
	s.messagesMux.Unlock()

	s.pendingMux.Lock()
	if _, exists := s.pending[message]; !exists {
		s.pending[message] = true
		s.pendingMux.Unlock()

		for _, neighbor := range s.neighbors {
			body := map[string]any{
				"type":    "broadcast",
				"message": message,
			}
			for i := 0; i < 3; i++ {
				if err := s.n.Send(neighbor, body); err == nil {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	} else {
		s.pendingMux.Unlock()
	}
}
