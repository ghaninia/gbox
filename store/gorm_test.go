package store

import (
	"context"
	"github.com/ghaninia/gbox/dto"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// newDBGormInstance returns a new instance of GormStore.
func newDBGormInstance() (IRepository, error) {
	return NewOutboxGormRepository(RepoSetting{
		TableName: "outbox",
	}, gormClient), nil
}

// TestOutboxGormRepository_GetTableName tests the GetTableName method of OutboxGormRepository.
func TestOutboxGormRepository_GetTableName(t *testing.T) {
	repo, err := newDBGormInstance()
	if err != nil {
		assert.NoErrorf(t, err, "error creating new instance of GormStore: %v", err)
		return
	}
	assert.Equal(t, "outbox", repo.GetTableName())
}

// TestOutboxGormRepository_NewRecords tests the NewRecords method of OutboxGormRepository.
func TestOutboxGormRepository_NewRecords(t *testing.T) {

	tearDownSuite := setupSuite(t)
	defer tearDownSuite(t)

	repo, err := newDBGormInstance()
	if err != nil {
		assert.NoErrorf(t, err, "error creating new instance of GormStore: %v", err)
		return
	}

	records := []dto.Outbox{
		{
			ID:         1,
			Payload:    "{'key': 'value'}",
			DriverName: "driver_name",
			State:      dto.OutboxStatePending,
			CreatedAt:  time.Now(),
		},
	}

	err = repo.NewRecords(context.Background(), records)
	assert.NoError(t, err)
}
