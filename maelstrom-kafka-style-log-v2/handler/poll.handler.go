package handler

import (
	"encoding/json"
	"maelstrom-kafka-style-log-v2/domain"
)

type PollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

func HandlePoll(node *domain.Node) func([]byte) ([]byte, error) {
	return func(msg []byte) ([]byte, error) {
		var request PollRequest
		if err := json.Unmarshal(msg, &request); err != nil {
			return nil, err
		}

		msgs := make(map[string][][]int)

		for key, offset := range request.Offsets {
			messages := node.GetLog(key, offset)

			pairs := make([][]int, len(messages))
			for i, msg := range messages {
				pairs[i] = []int{offset + i, msg}
			}

			if len(pairs) > 0 {
				msgs[key] = pairs
			}
		}

		response := PollResponse{
			Type: "poll_ok",
			Msgs: msgs,
		}

		return json.Marshal(response)
	}
}
