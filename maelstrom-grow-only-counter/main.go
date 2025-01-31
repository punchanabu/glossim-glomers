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

		delta := int(body["delta"].(float64))

		for {
			curr, err := seqKV.Read(context.Background(), "counter")
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					err = seqKV.CompareAndSwap(context.Background(), "counter", nil, delta, true)
					if err == nil {
						break
					}
					continue
				}
				return err
			}

			currInt := curr.(int)

			err = seqKV.CompareAndSwap(context.Background(), "counter", currInt, currInt+delta, false)
			if err == nil {
				break
			}
		}

		return node.Reply(msg, map[string]any{"type": "add_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		value, err := seqKV.Read(context.Background(), "counter")
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				return node.Reply(msg, map[string]any{"type": "read_ok", "value": 0})
			}
			return err
		}

		return node.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	node.Run()
}
