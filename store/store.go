package store

import (
	"context"
	"golang.org/x/sync/errgroup"
	"math/rand"
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
// It uses a mutex to ensure that the messages are saved in a thread-safe manner.
func (s *Store) Add(ctx context.Context, driverName string, messages ...dto.NewMessage) error {
	s.muMessages.Lock()
	defer s.muMessages.Unlock()

	for _, msg := range messages {
		var snowflakeID int64 = rand.Int63()
		outboxMessage := msg.ToOutBox(snowflakeID, driverName)
		s.messages = append(s.messages, outboxMessage)

		// check if the number of messages has reached the bulk size
		if len(s.messages) >= s.setting.BulkSize {
			if err := s.saveMessages(ctx, s.messages); err != nil {
				return err
			}
			// reset messages after saving
			s.messages = []dto.Outbox{}
		}
	}

	return nil
}

// saveMessages saves the messages in the outbox store.
func (s *Store) saveMessages(ctx context.Context, messages []dto.Outbox) error {
	if s.beforeSaveBatch != nil {
		if err := s.beforeSaveBatch(ctx, messages); err != nil {
			return err
		}
	}
	if err := s.repo.NewRecords(ctx, messages); err != nil {
		return err
	}
	if s.afterSaveBatch != nil {
		if err := s.afterSaveBatch(ctx, messages); err != nil {
			return err
		}
	}
	return nil
}

// AutoCommit starts a ticker that periodically saves the messages in the outbox store.
// It uses an error group to handle context cancellation and ensure that messages are saved before stopping the ticker.
// It also ensures that messages are saved in a thread-safe manner using a mutex.
// The ticker interval is set based on the setting provided in the store.
// If the context is done, it saves any remaining messages before stopping the ticker.
func (s *Store) AutoCommit(ctx context.Context) error {
	s.ticker = time.NewTicker(s.setting.IntervalTicker)

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for {
			select {
			case _ = <-ctx.Done():
				{
					s.muMessages.Lock()
					if err := s.saveMessages(ctx, s.messages); err != nil {
						s.muMessages.Unlock()
						return err
					}

					// reset messages after saving
					s.messages = []dto.Outbox{}
					s.muMessages.Unlock()
					s.ticker.Stop()
					return nil
				}
			case _ = <-s.ticker.C:
				{
					s.muMessages.Lock()
					if len(s.messages) == 0 {
						s.muMessages.Unlock()
						continue
					}

					if err := s.saveMessages(ctx, s.messages); err != nil {
						s.muMessages.Unlock()
						return err
					}

					// reset messages after saving
					s.messages = []dto.Outbox{}
					s.muMessages.Unlock()
				}
			}
		}
	})

	return group.Wait()
}
