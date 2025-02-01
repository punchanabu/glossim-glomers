package main

import (
	"log"
	"maelstrom-kafka-style-log/domain"
	"maelstrom-kafka-style-log/handler"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	maelstromNode := maelstrom.NewNode()

	node := &domain.Node{
		N:                 maelstromNode,
		Logs:              make(map[string]*domain.KafkaLog),
		CommitOffsetsList: make(map[string]int),
	}

	kafkaHandler := handler.NewKafkaHandler(maelstromNode, node)

	maelstromNode.Handle("send", kafkaHandler.Send)
	maelstromNode.Handle("poll", kafkaHandler.Poll)
	maelstromNode.Handle("commit_offsets", kafkaHandler.CommitOffsets)
	maelstromNode.Handle("list_committed_offsets", kafkaHandler.ListCommittedOffsets)

	if err := maelstromNode.Run(); err != nil {
		log.Fatal(err)
	}
}
