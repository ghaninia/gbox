package dto

import "encoding/json"

type NewMessage struct {
	Payload string `json:"payload"`
}

func (m NewMessage) ToString() string {
	s, _ := json.Marshal(m)
	return string(s)
}
