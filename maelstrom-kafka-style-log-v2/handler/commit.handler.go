package handler

import (
	"encoding/json"
	"maelstrom-kafka-style-log-v2/domain"
)

type CommitRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitResponse struct {
	Type string `json:"type"`
}

func HandleCommit(node *domain.Node) func([]byte) ([]byte, error) {
	return func(msg []byte) ([]byte, error) {
		var request CommitRequest
		if err := json.Unmarshal(msg, &request); err != nil {
			return nil, err
		}

		for key, offset := range request.Offsets {
			node.Commit(key, offset)
		}

		return json.Marshal(CommitResponse{
			Type: "commit_offsets_ok",
		})
	}
}
