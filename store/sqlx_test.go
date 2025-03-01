package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// newOutboxRedisRepoInstance returns a new instance of RedisStore.
func newDBSqlxInstance() (IRepository, error) {
	return NewOutboxSqlxRepository(RepoSetting{
		TableName: "outbox",
	}, sqlxClient), nil
}

// TestOutboxSqlxRepository_GetTableName tests the OutboxSqlxRepository.
func TestOutboxSqlxRepository_GetTableName(t *testing.T) {
	repo, err := newDBSqlxInstance()
	if err != nil {
		assert.NoErrorf(t, err, "error creating new instance of OutboxSqlxRepository")
		return
	}
	assert.Equalf(t, repo.GetTableName(), "outbox", "table name should be outbox")
}

// TestOutboxSqlxRepository_NewRecords tests the method NewRecords of OutboxSqlxRepository.
func TestOutboxSqlxRepository_NewRecords(t *testing.T) {

	tearDownSuite := setupSuite(t)
	defer tearDownSuite(t)

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
			State:      OutboxStateInProgress,
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
