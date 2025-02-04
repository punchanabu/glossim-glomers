package handler

import (
	"encoding/json"
	"fmt"
	"maelstrom-kafka-style-log-v2/domain"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func HandleCommit(node *domain.Node) func(maelstrom.Message) (map[string]any, error) {
	return func(msg maelstrom.Message) (map[string]any, error) {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil, err
		}

		rawOffsets, ok := body["offsets"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("offsets must be a map")
		}

		offsets := make(map[string]int)
		for k, v := range rawOffsets {
			if val, ok := v.(float64); ok {
				if val < 0 {
					return nil, fmt.Errorf("invalid negative offset %d for key %s", int(val), k)
				}
				offsets[k] = int(val)
			}
		}

		for key, offset := range offsets {
			node.Commit(key, offset)
		}

		return map[string]any{
			"type": "commit_offsets_ok",
		}, nil
	}
}
