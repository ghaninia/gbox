package store

import (
	"context"
	"golang.org/x/sync/errgroup"
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
	NodeID         int
	DriverName     string
	BulkSize       int
	IntervalTicker time.Duration
}

type IRepository interface {
	GetTableName() string
	NewRecords(ctx context.Context, records []dto.Outbox) error
}

type IStore interface {
	Add(ctx context.Context, driverName string, messages ...dto.NewMessage) error
	AutoCommit(ctx context.Context) error
}

type Store struct {
	setting         Setting
	repo            IRepository
	muMessages      sync.Mutex
	ticker          *time.Ticker
	messages        []dto.Outbox
	afterSaveBatch  func(ctx context.Context, messages []dto.Outbox) error
	beforeSaveBatch func(ctx context.Context, messages []dto.Outbox) error
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

	// if a beforeSaveBatch function is defined, call it
	if s.beforeSaveBatch != nil {
		if err := s.beforeSaveBatch(ctx, s.messages); err != nil {
			return err
		}
	}

	// check if the number of messages has reached the bulk size
	if len(s.messages) >= s.setting.BulkSize {
		if err := s.repo.NewRecords(ctx, s.messages); err != nil {
			return err
		}
		if s.afterSaveBatch != nil {
			if err := s.afterSaveBatch(ctx, s.messages); err != nil {
				return err
			}
		}
		s.messages = []dto.Outbox{}
	} else {
		return nil
	}

	return nil
}

// AutoCommit starts a ticker that periodically saves the messages in the outbox store.
func (s *Store) AutoCommit(ctx context.Context) error {
	s.ticker = time.NewTicker(s.setting.IntervalTicker)

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for {
			select {
			case _ = <-ctx.Done():
			case _ = <-s.ticker.C:
				s.muMessages.Lock()
				if s.beforeSaveBatch != nil {
					if err := s.beforeSaveBatch(ctx, s.messages); err != nil {
						return err
					}
				}
				if err := s.repo.NewRecords(ctx, s.messages); err != nil {
					return err
				}
				if s.afterSaveBatch != nil {
					if err := s.afterSaveBatch(ctx, s.messages); err != nil {
						return err
					}
				}
				s.messages = []dto.Outbox{}
				s.muMessages.Unlock()
			}
		}
	})

	return group.Wait()
}
