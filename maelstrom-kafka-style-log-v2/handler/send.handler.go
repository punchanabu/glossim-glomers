package handler

import (
	"encoding/json"
	"fmt"
	"maelstrom-kafka-style-log-v2/domain"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func HandleSend(node *domain.Node) func(maelstrom.Message) (map[string]any, error) {
	return func(msg maelstrom.Message) (map[string]any, error) {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil, err
		}

		key, ok := body["key"].(string)
		if !ok {
			return nil, fmt.Errorf("key must be a string")
		}

		msgVal, ok := body["msg"].(float64)
		if !ok {
			return nil, fmt.Errorf("msg must be a number")
		}

		offset := node.AddLog(key, int(msgVal))

		return map[string]any{
			"type":   "send_ok",
			"offset": offset,
		}, nil
	}
}
