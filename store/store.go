package store

import (
	"context"
	"time"
)

type Setting struct {
	TableName string
}

type Outbox struct {
	ID               int64           `gorm:"id" db:"id" json:"id"`
	Payload          string          `gorm:"payload" db:"payload" json:"payload"`
	DriverName       string          `gorm:"driver_name" db:"driver_name" json:"driver_name"`
	State            OutboxStateEnum `gorm:"state" db:"state" json:"state"`
	CreatedAt        time.Time       `gorm:"created_at" db:"created_at" json:"created_at"`
	LockedAt         *time.Time      `gorm:"locked_at" db:"locked_at" json:"locked_at"`
	LockedBy         *string         `gorm:"locked_by" db:"locked_by" json:"locked_by"`
	LastAttemptedAt  *time.Time      `gorm:"last_attempted_at" db:"last_attempted_at" json:"last_attempted_at"`
	NumberOfAttempts *int64          `gorm:"number_of_attempts" db:"number_of_attempts" json:"number_of_attempts"`
	Error            *string         `gorm:"error" db:"error" json:"error"`
}

type OutboxStateEnum string

const (
	OutboxStatePending    OutboxStateEnum = "PENDING"
	OutboxStateINPROGRESS OutboxStateEnum = "IN-PROGRESS"
	OutboxStateSucceed    OutboxStateEnum = "SUCCEED"
	OutboxStateFailed     OutboxStateEnum = "FAILED"
)

type IStore interface {
	GetTableName() string
	NewRecords(ctx context.Context, records []Outbox) error
}
