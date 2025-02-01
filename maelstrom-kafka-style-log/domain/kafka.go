package domain

type KafkaLog struct {
	Key      string
	Messages []int
}

func (k *KafkaLog) Append(message int) int {
	k.Messages = append(k.Messages, message)
	return len(k.Messages) - 1
}

func (k *KafkaLog) GetMessagesByOffset(offset int) []int {
	if offset >= len(k.Messages) {
		return []int{}
	}
	return k.Messages[offset:]
}

func (k *KafkaLog) CommitOffset(offset int) {
	k.Messages = k.Messages[:offset]
}
