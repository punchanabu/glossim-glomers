package handler

import (
	"encoding/json"
	"maelstrom-kafka-style-log-v2/domain"
)

type SendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type SendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

func HandleSend(node *domain.Node) func([]byte) ([]byte, error) {
	return func(msg []byte) ([]byte, error) {
		var request SendRequest
		if err := json.Unmarshal(msg, &request); err != nil {
			return nil, err
		}

		offset := node.AddLog(request.Key, request.Msg)

		response := SendResponse{
			Type:   "send_ok",
			Offset: offset,
		}

		return json.Marshal(response)
	}
}
