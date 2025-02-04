package main

import (
	"log"
	"maelstrom-kafka-style-log-v2/domain"
	"maelstrom-kafka-style-log-v2/handler"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	node := domain.NewNode()

	n.Handle("send", func(msg maelstrom.Message) error {
		response, err := handler.HandleSend(node)(msg.Body)
		if err != nil {
			return err
		}
		return n.Reply(msg, response)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		response, err := handler.HandlePoll(node)(msg.Body)
		if err != nil {
			return err
		}
		return n.Reply(msg, response)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		response, err := handler.HandleCommit(node)(msg.Body)
		if err != nil {
			return err
		}
		return n.Reply(msg, response)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		response, err := handler.HandleList(node)(msg.Body)
		if err != nil {
			return err
		}
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
