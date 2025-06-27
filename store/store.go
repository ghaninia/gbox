package store

import (
	"context"
	"github.com/ghaninia/gbox/dto"
	"sync"
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
}

type IRepository interface {
	GetTableName() string
	NewRecords(ctx context.Context, records []dto.Outbox) error
}

type IStore interface {
	Add(ctx context.Context, driverName string, messages ...dto.NewMessage) error
}

type Store struct {
	setting    Setting
	repo       IRepository
	muMessages sync.Mutex
	messages   []dto.Outbox
	afterSave  func(ctx context.Context, messages []dto.Outbox) error
	beforeSave func(ctx context.Context, messages []dto.Outbox) error
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

// Add adds new messages to the outbox store.
// It maps the NewMessage to Outbox messages, checks if the bulk size is reached,
func (s *Store) Add(ctx context.Context, driverName string, messages ...dto.NewMessage) error {

	s.muMessages.Lock()
	defer s.muMessages.Unlock()

	if len(messages) == 0 {
		return nil
	}

	// map NewMessage to Outbox messages
	for _, msg := range messages {
		outboxMessage := msg.ToOutBox(int64(len(s.messages)+1), driverName)
		s.messages = append(s.messages, outboxMessage)
	}

	// if a beforeSave function is defined, call it
	if s.beforeSave != nil {
		if err := s.beforeSave(ctx, s.messages); err != nil {
			return err
		}
	}

	// check if the number of messages has reached the bulk size
	if len(s.messages) >= s.setting.BulkSize {
		if err := s.repo.NewRecords(ctx, s.messages); err != nil {
			return err
		}
		if s.afterSave != nil {
			if err := s.afterSave(ctx, s.messages); err != nil {
				return err
			}
		}
		s.messages = []dto.Outbox{}
	} else {
		return nil
	}

	return nil
}
