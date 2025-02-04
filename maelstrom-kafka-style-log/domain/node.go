package domain

import maelstrom "github.com/jepsen-io/maelstrom/demo/go"

type Node struct {
	N                 *maelstrom.Node
	Logs              map[string]*KafkaLog
	CommitOffsetsList map[string]int
}

func (n *Node) Send(key string, message int) int {
	if _, exists := n.Logs[key]; !exists {
		n.Logs[key] = &KafkaLog{
			Key: key,
		}
	}

	return n.Logs[key].Append(message)
}

func (n *Node) GetMessagesByOffset(offsets map[string]int) map[string][][2]int {
	messages := make(map[string][][2]int)
	for key, offset := range offsets {
		if log, exists := n.Logs[key]; exists {
			messages[key] = log.GetMessagesByOffset(offset)
		} else {
			messages[key] = [][2]int{}
		}
	}
	return messages
}

func (n *Node) CommitOffsets(offsets map[string]int) {
	// Initialize CommitOffsetsList if nil
	if n.CommitOffsetsList == nil {
		n.CommitOffsetsList = make(map[string]int)
	}

	for key, offset := range offsets {
		if offset < 0 {
			continue
		}

		// Always store the commit offset
		n.CommitOffsetsList[key] = offset

		// Create log if it doesn't exist
		if _, exists := n.Logs[key]; !exists {
			n.Logs[key] = &KafkaLog{
				Key:      key,
				Messages: make([]int, 0),
			}
		}

		// Update the log's commit point
		n.Logs[key].CommitOffset(offset)
	}
}

func (n *Node) GetCommitOffsets(keys []interface{}) map[string]int {
	commitOffsets := make(map[string]int)
	for _, rawKey := range keys {
		if key, ok := rawKey.(string); ok {
			if offset, exists := n.CommitOffsetsList[key]; exists {
				commitOffsets[key] = offset
			}
		}
	}
	return commitOffsets
}
