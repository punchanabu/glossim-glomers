package handler

import (
	"encoding/json"
	"fmt"
	"maelstrom-kafka-style-log-v2/domain"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func HandleList(node *domain.Node) func(maelstrom.Message) (map[string]any, error) {
	return func(msg maelstrom.Message) (map[string]any, error) {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil, err
		}

		rawKeys, ok := body["keys"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("keys must be an array")
		}

		offsets := make(map[string]int)
		for _, rawKey := range rawKeys {
			key, ok := rawKey.(string)
			if !ok {
				return nil, fmt.Errorf("each key must be a string")
			}
			offset := node.GetCommitted(key)
			if offset != 0 { 
				offsets[key] = offset
			}
		}

		return map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		}, nil
	}
}
