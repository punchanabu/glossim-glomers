package domain

import (
	"sync"
)

type Node struct {
	mu         sync.RWMutex
	LogStorage map[string]*Log
	Committed  map[string]int
}

func NewNode() *Node {
	return &Node{
		LogStorage: make(map[string]*Log),
		Committed:  make(map[string]int),
	}
}

func (n *Node) AddLog(key string, message int) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.LogStorage[key]; !ok {
		n.LogStorage[key] = &Log{
			Messages: []int{},
		}
	}

	offset := len(n.LogStorage[key].Messages)
	n.LogStorage[key].Messages = append(n.LogStorage[key].Messages, message)
	return offset
}

func (n *Node) GetLog(key string, offset int) []int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if log, exists := n.LogStorage[key]; exists {
		if offset >= len(log.Messages) {
			return []int{}
		}
		messages := make([]int, len(log.Messages[offset:]))
		copy(messages, log.Messages[offset:])
		return messages
	}
	return []int{}
}

func (n *Node) Commit(key string, offset int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Committed == nil {
		n.Committed = make(map[string]int)
	}
	n.Committed[key] = offset
}

func (n *Node) GetCommitted(key string) int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.Committed == nil {
		return 0
	}

	if offset, exists := n.Committed[key]; exists {
		return offset
	}
	return 0
}
