package handler

import (
	"encoding/json"
	"fmt"
	"maelstrom-kafka-style-log-v2/domain"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type PollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

func HandlePoll(node *domain.Node) func(maelstrom.Message) (map[string]any, error) {
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
				offsets[k] = int(val)
			}
		}

		msgs := make(map[string][][]int)
		for key, offset := range offsets {
			messages := node.GetLog(key, offset)
			pairs := make([][]int, len(messages))
			for i, msg := range messages {
				pairs[i] = []int{offset + i, msg}
			}
			if len(pairs) > 0 {
				msgs[key] = pairs
			}
		}

		return map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		}, nil
	}
}
