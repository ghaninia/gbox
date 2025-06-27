package dto

import (
	"encoding/json"
	"time"
)

type NewMessage struct {
	Payload string `json:"payload"`
}

func (m NewMessage) ToString() string {
	s, _ := json.Marshal(m)
	return string(s)
}

func (m NewMessage) ToOutBox(ID int64, driverName string) Outbox {
	return Outbox{
		ID:               ID,
		DriverName:       driverName,
		Payload:          m.ToString(),
		State:            OutboxStatePending,
		CreatedAt:        time.Now(),
		LockedAt:         nil,
		LockedBy:         nil,
		LastAttemptedAt:  nil,
		NumberOfAttempts: nil,
		Error:            nil,
	}
}
