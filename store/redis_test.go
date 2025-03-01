package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// newOutboxRedisRepoInstance returns a new instance of RedisStore.
func newOutboxRedisRepoInstance() (IRepository, error) {
	return NewOutboxRedisRepository(RepoSetting{
		TableName: "outbox",
	}, redisClient), nil
}

// TestOutboxRedisRepository_NewRecords tests the method NewRecords of OutboxRedisRepository.
func TestOutboxRedisRepository_NewRecords(t *testing.T) {
	repo, err := newOutboxRedisRepoInstance()
	if err != nil {
		assert.FailNowf(t, "failed to create new instance of OutboxRedisRepository", "%v", err)
		return
	}

	err = repo.NewRecords(context.Background(), []Outbox{
		{ID: 1, Payload: "Hello, World!"},
		{ID: 2, Payload: "Hello, Universe!"},
	})

	assert.NoErrorf(t, err, "failed to insert new records to outbox table")
}

// TestOutboxRedisRepository_GetTableName tests the method GetTableName of OutboxRedisRepository.
func TestOutboxRedisRepository_GetTableName(t *testing.T) {
	repo, err := newOutboxRedisRepoInstance()
	if err != nil {
		assert.FailNowf(t, "failed to create new instance of OutboxRedisRepository", "%v", err)
		return
	}

	assert.Equal(t, "outbox", repo.GetTableName())
}

// TestNewOutboxRedisRepository tests the method NewOutboxRedisRepository.
func TestNewOutboxRedisRepository(t *testing.T) {
	client, err := newRedisTestContainerClient()
	if err != nil {
		assert.FailNowf(t, "failed to create new instance of Redis client", "%v", err)
		return
	}

	repo := NewOutboxRedisRepository(RepoSetting{
		TableName: "outbox",
	}, client)

	assert.NotNil(t, repo)
}
