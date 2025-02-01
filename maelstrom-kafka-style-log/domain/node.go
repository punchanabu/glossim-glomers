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

func (n *Node) GetMessagesByOffset(offsets map[string]int) map[string]any {
	messages := make(map[string]any)
	for key, offset := range offsets {
		if _, exists := n.Logs[key]; !exists {
			messages[key] = []string{}
			continue
		}

		messages[key] = n.Logs[key].GetMessagesByOffset(offset)
	}
	return messages
}

func (n *Node) CommitOffsets(offsets map[string]int) {
	for key, offset := range offsets {
		if _, exists := n.Logs[key]; !exists {
			continue
		}

		n.Logs[key].CommitOffset(offset)
		n.CommitOffsetsList[key] = offset
	}
}

func (n *Node) GetCommitOffsets(keys []string) map[string]int {
	commitOffsets := make(map[string]int)
	for _, key := range keys {
		if offset, ok := n.CommitOffsetsList[key]; ok {
			commitOffsets[key] = offset
		}
	}
	return commitOffsets
}
