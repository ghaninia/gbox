package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ghaninia/gbox/dto"
)

var (
	defaultSetting = Setting{
		BatchInsertEnable: true,
		BatchInsertSize:   100,
		MessageBufferSize: 1000,
		TickerTimeout:     5 * time.Second,
	}
)

type RepoSetting struct {
	TableName string
}

type Setting struct {
	NodeID            int
	DriverName        string
	BatchInsertEnable bool
	BatchInsertSize   int
	MessageBufferSize int
	TickerTimeout     time.Duration
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
	OutboxStateInProgress OutboxStateEnum = "IN-PROGRESS"
	OutboxStateSucceed    OutboxStateEnum = "SUCCEED"
	OutboxStateFailed     OutboxStateEnum = "FAILED"
)

type IRepository interface {
	GetTableName() string
	NewRecords(ctx context.Context, records []Outbox) error
}

type IStore interface {
	Dispatch(ctx context.Context, msg dto.NewMessage) error
	Close()
}

type Store struct {
	setting     Setting
	repo        IRepository
	wg          sync.WaitGroup
	startOnce   sync.Once
	messageChan chan Outbox
	stopChan    chan struct{}
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
		repo:        repo,
		setting:     s,
		messageChan: make(chan Outbox, s.MessageBufferSize),
		stopChan:    make(chan struct{}),
	}
}

// Dispatch sends a new message to the store for processing.
func (s *Store) Dispatch(ctx context.Context, msg dto.NewMessage) error {

	newMessage := Outbox{
		Payload:    msg.ToString(),
		DriverName: s.setting.DriverName,
		State:      OutboxStatePending,
		CreatedAt:  time.Now(),
	}

	// lazy start the message processing loop
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go s.processBatchMessages(ctx)
	})

	select {
	case s.messageChan <- newMessage:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return s.repo.NewRecords(ctx, []Outbox{newMessage})
	}
}

// Close stops the store's message processing loop and closes the message and stop channels.
func (s *Store) Close() {
	close(s.stopChan)
	close(s.messageChan)
	s.wg.Wait()
}

// processBatchMessages processes messages in batches.
func (s *Store) processBatchMessages(ctx context.Context) {
	defer s.wg.Done()

	if !s.setting.BatchInsertEnable {
		return
	}

	var batch []Outbox
	batchSize := s.setting.BatchInsertSize
	tickerTimeout := s.setting.TickerTimeout

	ticker := time.NewTicker(tickerTimeout)
	defer ticker.Stop()

	for {
		select {
		case msg := <-s.messageChan:
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				if err := s.saveBatch(ctx, batch); err != nil {
					fmt.Println("Error saving batch:", err)
				} else {
					batch = nil
				}
			}

		case <-ticker.C:
			if len(batch) > 0 {
				if err := s.saveBatch(ctx, batch); err != nil {
					fmt.Println("Error saving batch on timer:", err)
				} else {
					batch = nil
				}
			}

		case <-s.stopChan:
			if len(batch) > 0 {
				if err := s.saveBatch(ctx, batch); err != nil {
					fmt.Println("Error saving remaining batch:", err)
				}
			}
			return
		}
	}
}

// saveBatch saves a batch of messages to the repository.
func (s *Store) saveBatch(ctx context.Context, batch []Outbox) error {
	err := s.repo.NewRecords(ctx, batch)
	if err != nil {
		return err
	}

	return nil
}
