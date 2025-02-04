package handler

import (
	"encoding/json"
	"maelstrom-kafka-style-log/domain"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaHandler struct {
	node   *maelstrom.Node
	domain domain.NodeActions
}

func NewKafkaHandler(node *maelstrom.Node, domain domain.NodeActions) *KafkaHandler {
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

	msgVal, ok := body["msg"].(float64)
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "msg must be a number",
		})
	}

	offset := h.domain.Send(key, int(msgVal))
	return h.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (h *KafkaHandler) Poll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	rawOffsets, ok := body["offsets"].(map[string]interface{})
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "offsets must be a map",
		})
	}

	offsets := make(map[string]int)
	for k, v := range rawOffsets {
		if val, ok := v.(float64); ok {
			offsets[k] = int(val)
		}
	}

	msgs := h.domain.GetMessagesByOffset(offsets)
	return h.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (h *KafkaHandler) CommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	rawOffsets, ok := body["offsets"].(map[string]interface{})
	if !ok {
		return h.node.Reply(msg, map[string]any{
			"type":  "error",
			"error": "offsets must be a map",
		})
	}

	offsets := make(map[string]int)
	for k, v := range rawOffsets {
		if val, ok := v.(float64); ok {
			offsets[k] = int(val)
		}
	}

	h.domain.CommitOffsets(offsets)
	return h.node.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
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

	offsets := h.domain.GetCommitOffsets(rawKeys)
	return h.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}
