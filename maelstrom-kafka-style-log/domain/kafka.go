package domain

type KafkaLog struct {
	Key      string
	Messages []int
}

type Message struct {
	Offset int `json:"offset"`
	Msg    int `json:"msg"`
}

func (k *KafkaLog) Append(message int) int {
	if k.Messages == nil {
		k.Messages = make([]int, 0)
	}
	k.Messages = append(k.Messages, message)
	return len(k.Messages) - 1
}

func (k *KafkaLog) GetMessagesByOffset(offset int) [][2]int {
	if k.Messages == nil || offset >= len(k.Messages) {
		return [][2]int{}
	}
	messages := make([][2]int, 0)
	for i := offset; i < len(k.Messages); i++ {
		messages = append(messages, [2]int{i, k.Messages[i]})
	}
	return messages
}

func (k *KafkaLog) CommitOffset(offset int) {
	if k.Messages == nil {
		k.Messages = make([]int, 0)
		return
	}

	// Don't modify messages array, just validate offset
	if offset < 0 {
		return
	}

	// Ensure offset is within bounds
	if offset > len(k.Messages) {
		offset = len(k.Messages)
	}

	// We don't actually truncate messages anymore, just keep track of the commit point
	// The messages array stays intact
}
