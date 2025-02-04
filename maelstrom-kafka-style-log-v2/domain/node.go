package domain

type Node struct {
	LogStorage map[string]*Log
}

func NewNode() *Node {
	return &Node{
		LogStorage: make(map[string]*Log),
	}
}

func (n *Node) AddLog(key string, message int) int {
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
	if log, exists := n.LogStorage[key]; exists {
		if offset >= len(log.Messages) {
			return []int{}
		}
		return log.Messages[offset:]
	}
	return []int{}
}
