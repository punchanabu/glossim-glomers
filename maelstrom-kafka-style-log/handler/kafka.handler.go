package handler

import (
	"encoding/json"
	"maelstrom-kafka-style-log/domain"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaHandler struct {
	node   *maelstrom.Node
	domain *domain.Node
}

func NewKafkaHandler(node *maelstrom.Node, domain *domain.Node) *KafkaHandler {
	return &KafkaHandler{
		node:   node,
		domain: domain,
	}
}

func (h *KafkaHandler) Send(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key, ok := body["key"].(string)
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "key must be a string",
		})
	}

	message, ok := body["message"].(int)
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "message must be a int",
		})
	}

	offset := h.domain.Send(key, message)

	return h.node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func (h *KafkaHandler) Poll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets, ok := body["offsets"].(map[string]int)
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "offsets must be a map[string]int",
		})
	}

	messages := h.domain.GetMessagesByOffset(offsets)
	return h.node.Reply(msg, map[string]any{
		"type":     "poll_ok",
		"messages": messages,
	})
}

func (h *KafkaHandler) CommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets, ok := body["offsets"].(map[string]int)
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "offsets must be a map[string]int",
		})
	}

	h.domain.CommitOffsets(offsets)

	return h.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func (h *KafkaHandler) ListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	rawKeys, ok := body["keys"].([]interface{})
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "keys must be an array",
		})
	}

	// Convert keys to strings
	keys := make([]string, len(rawKeys))
	for i, k := range rawKeys {
		if str, ok := k.(string); ok {
			keys[i] = str
		} else {
			return h.node.Reply(msg, map[string]any{
				"type":  "error",
				"error": "all keys must be strings",
			})
		}
	}

	offsets := h.domain.GetCommitOffsets(keys)

	return h.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}
