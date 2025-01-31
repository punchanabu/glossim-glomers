package main

import (
	"context"
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	seqKV := maelstrom.NewSeqKV(node)

	node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		curr, err := seqKV.ReadInt(context.Background(), "counter")
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				curr = 0
			} else {
				return err
			}
		}

		seqKV.Write(context.Background(), "counter", curr+body["delta"].(int))

		return node.Reply(msg, map[string]any{"type": "add_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		value, err := seqKV.ReadInt(context.Background(), "counter")
		if err != nil {
			return err
		}

		return node.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	node.Run()
}
