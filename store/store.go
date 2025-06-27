package store

import (
	"context"
	"sync"
	"time"

	"github.com/ghaninia/gbox/dto"
)

var (
	defaultSetting = Setting{}
)

type RepoSetting struct {
	TableName string
}

type Setting struct {
	NodeID     int
	DriverName string
	BulkSize   int
	FlushTimer time.Duration
}

type Outbox struct {
	ID               int64           `gorm:"id" db:"id" json:"id"`
	DriverName       string          `gorm:"driver_name" db:"driver_name" json:"driver_name"`
	Payload          string          `gorm:"payload" db:"payload" json:"payload"`
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
	OutboxStateInProgress OutboxStateEnum = "IN-PROGRESS"
	OutboxStateSucceed    OutboxStateEnum = "SUCCEED"
	OutboxStateFailed     OutboxStateEnum = "FAILED"
)

type IRepository interface {
	GetTableName() string
	NewRecords(ctx context.Context, records []Outbox) error
}

type IStore interface {
	Add(ctx context.Context, msgs ...dto.NewMessage) error
}

type Store struct {
	setting    Setting
	repo       IRepository
	muMessages sync.Mutex
	messages   []Outbox
	afterSave  func(messages []Outbox) error
	beforeSave func(messages []Outbox) error
}

// NewStore creates a new store instance with the provided repository and settings.
// If no settings are provided, the default settings are used.
func NewStore(repo IRepository, settings ...Setting) IStore {
	var s = func() Setting {
		if len(settings) > 0 {
			return settings[0]
		}
		return defaultSetting
	}()
	return &Store{
		repo:    repo,
		setting: s,
	}
}

func (s Store) Add(ctx context.Context, msgs ...dto.NewMessage) error {
	//TODO implement me
	panic("implement me")
}
