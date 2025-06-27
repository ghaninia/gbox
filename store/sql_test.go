package store

import (
	"context"
	"github.com/ghaninia/gbox/dto"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// newDBSqlInstance returns a new instance of OutboxSqlRepository.
func newDBSqlInstance() (IRepository, error) {
	return NewOutboxSqlRepository(RepoSetting{
		TableName: "outbox",
	}, sqlClient), nil
}

// TestOutboxSqlRepository_GetTableName tests the GetTableName method of OutboxSqlRepository.
func TestOutboxSqlRepository_GetTableName(t *testing.T) {
	repo, err := newDBSqlInstance()
	if err != nil {
		assert.NoErrorf(t, err, "error creating new instance of OutboxSqlRepository")
		return
	}
	assert.Equalf(t, repo.GetTableName(), "outbox", "table name should be outbox")
}

// TestOutboxSqlRepository_NewRecords tests the method NewRecords of OutboxSqlxRepository.
func TestOutboxSqlRepository_NewRecords(t *testing.T) {

	tearDownSuite := setupSuite(t)
	defer tearDownSuite(t)

	repo, err := newDBSqlInstance()
	if err != nil {
		assert.NoErrorf(t, err, "error creating new instance of OutboxSqlxRepository")
		return
	}

	records := []dto.Outbox{
		{
			ID:         1,
			Payload:    `{"name": "John Doe"}`,
			DriverName: "grpc",
			State:      dto.OutboxStateInProgress,
			CreatedAt:  time.Now(),
		}, {
			ID:         2,
			Payload:    `{"name": "Jane Doe"}`,
			DriverName: "http",
			State:      dto.OutboxStatePending,
			CreatedAt:  time.Now(),
		}, {
			ID:         3,
			Payload:    `{"name": "John Doe"}`,
			DriverName: "grpc",
			State:      dto.OutboxStateSucceed,
			CreatedAt:  time.Now(),
		},
	}

	err = repo.NewRecords(context.Background(), records)
	assert.NoError(t, err)
}
