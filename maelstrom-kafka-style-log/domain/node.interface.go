package domain

type NodeActions interface {
	Send(key string, message int) int
	GetMessagesByOffset(offsets map[string]int) map[string][][2]int
	CommitOffsets(offsets map[string]int)
	GetCommitOffsets(keys []interface{}) map[string]int
}
