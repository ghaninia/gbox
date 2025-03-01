package store

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// newOutboxRedisRepoInstance returns a new instance of RedisStore.
func newDBSqlxInstance() (IStore, error) {
	return NewOutboxSqlxRepository(Setting{
		TableName: "outbox",
	}, sqlxClient), nil
}

// TestOutboxSqlxRepository tests the OutboxSqlxRepository.
func TestOutboxSqlxRepository_GetTableName(t *testing.T) {
	repo, err := newDBSqlxInstance()
	if err != nil {
		assert.NoErrorf(t, err, "error creating new instance of OutboxSqlxRepository")
		return
	}
	assert.Equalf(t, repo.GetTableName(), "outbox", "table name should be outbox")
}

func TestOutboxSqlxRepository_NewRecords(t *testing.T) {
	repo, err := newDBSqlxInstance()
	if err != nil {
		assert.NoErrorf(t, err, "error creating new instance of OutboxSqlxRepository")
		return
	}

	records := []Outbox{
		{
			ID:         1,
			Payload:    `{"name": "John Doe"}`,
			DriverName: "grpc",
			State:      OutboxStateINPROGRESS,
			CreatedAt:  time.Now(),
		}, {
			ID:         2,
			Payload:    `{"name": "Jane Doe"}`,
			DriverName: "http",
			State:      OutboxStatePending,
			CreatedAt:  time.Now(),
		}, {
			ID:         3,
			Payload:    `{"name": "John Doe"}`,
			DriverName: "grpc",
			State:      OutboxStateSucceed,
			CreatedAt:  time.Now(),
		},
	}

	err = repo.NewRecords(context.Background(), records)
	assert.NoError(t, err)
}
