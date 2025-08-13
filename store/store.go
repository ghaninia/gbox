package store

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ghaninia/gbox/dto"
)

var (
	defaultSetting = Setting{
		BackoffDelay: 5 * time.Millisecond,
	}
)

type RepoSetting struct {
	TableName string
}

type Setting struct {
	NodeID             int
	DriverName         string
	BatchInsertEnabled bool
	MaxBatchSize       int
	IntervalTicker     time.Duration
	BackoffEnabled     bool
	BackoffMaxRetries  int
	BackoffDelay       time.Duration
}

type IRepository interface {
	GetTableName() string
	NewRecords(ctx context.Context, records []dto.Outbox) error
	FetchMessages(ctx context.Context, limit int) ([]dto.Outbox, error)
}

type IStore interface {
	Add(ctx context.Context, driverName string, messages ...dto.NewMessage) error
	AutoCommit(ctx context.Context) error
	SetBeforeSaveBatch(f func(ctx context.Context, messages []dto.Outbox) error)
	SetAfterSaveBatch(f func(ctx context.Context, messages []dto.Outbox) error)
	Messages() []dto.Outbox
	FetchMessages(ctx context.Context, limit int) ([]dto.Outbox, error)
	MarkAsProcessed(ctx context.Context, id int64) error
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
func (s *Store) Add(ctx context.Context, driverName string, messages ...dto.NewMessage) error {
	s.muMessages.Lock()
	defer s.muMessages.Unlock()

	for _, msg := range messages {

		// TODO: added snowflakeID generation logic
		var snowflakeID int64 = rand.Int63()

		outboxMessage := msg.ToOutBox(snowflakeID, driverName)

		if !s.setting.BatchInsertEnabled {
			if err := s.saveMessages(ctx, s.messages); err != nil {
				return err
			}
			break
		}

		s.messages = append(s.messages, outboxMessage)

		// check if the number of messages has reached the bulk size
		if len(s.messages) >= s.setting.MaxBatchSize {
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
func (s *Store) saveMessages(ctx context.Context, messages []dto.Outbox) (err error) {
	if s.beforeSaveBatch != nil {
		if err = s.beforeSaveBatch(ctx, messages); err != nil {
			return err
		}
	}
	if err = s.repo.NewRecords(ctx, messages); err != nil {
		if !s.setting.BackoffEnabled {
			return err
		}
		for i := 0; i < s.setting.BackoffMaxRetries; i++ {
			time.Sleep(s.setting.BackoffDelay)
			if err = s.repo.NewRecords(ctx, messages); err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
	}
	if s.afterSaveBatch != nil {
		if err = s.afterSaveBatch(ctx, messages); err != nil {
			return err
		}
	}
	return nil
}

// AutoCommit starts a ticker that periodically saves the messages in the outbox store.
func (s *Store) AutoCommit(ctx context.Context) error {
	s.ticker = time.NewTicker(s.setting.IntervalTicker)
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
}

// SetBeforeSaveBatch sets a function to be called before saving a batch of messages.
func (s *Store) SetBeforeSaveBatch(f func(ctx context.Context, messages []dto.Outbox) error) {
	s.beforeSaveBatch = f
}

// SetAfterSaveBatch sets a function to be called after saving a batch of messages.
func (s *Store) SetAfterSaveBatch(f func(ctx context.Context, messages []dto.Outbox) error) {
	s.afterSaveBatch = f
}

// Messages returns the current messages in the outbox store.
func (s *Store) Messages() []dto.Outbox {
	s.muMessages.Lock()
	defer s.muMessages.Unlock()
	return s.messages
}

// FetchMessages fetches messages from the repository with a limit.
func (s *Store) FetchMessages(ctx context.Context, limit int) ([]dto.Outbox, error) {
	return s.repo.FetchMessages(ctx, limit)
}

func (s *Store) MarkAsProcessed(ctx context.Context, id int64) error {
	panic("implement me")
}
