package main

import (
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageNode struct {
	n           *maelstrom.Node
	messages    map[int]bool
	neighbors   []string
	messagesMux sync.RWMutex
}
