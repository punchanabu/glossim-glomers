package handler

import (
	"encoding/json"
	"maelstrom-kafka-style-log-v2/domain"
)

type ListRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func HandleList(node *domain.Node) func([]byte) ([]byte, error) {
	return func(msg []byte) ([]byte, error) {
		var request ListRequest
		if err := json.Unmarshal(msg, &request); err != nil {
			return nil, err
		}

		offsets := make(map[string]int)
		for _, key := range request.Keys {
			offsets[key] = node.GetCommitted(key)
		}

		return json.Marshal(ListResponse{
			Type:    "list_committed_offsets_ok",
			Offsets: offsets,
		})
	}
}
