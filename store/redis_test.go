package store

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	testContainerRedis "github.com/testcontainers/testcontainers-go/modules/redis"
)

var (
	redisClient *redis.Client
	onceRedis   sync.Once
)

// newRedisTestContainerClient returns a new instance of Redis client.
func newRedisTestContainerClient() (_ *redis.Client, err error) {

	onceRedis.Do(func() {

		var (
			ctx              = context.Background()
			conn             *testContainerRedis.RedisContainer
			parsedURL        *url.URL
			connectionString string
		)

		conn, err = testContainerRedis.Run(ctx, "redis:7-alpine",
			testContainerRedis.WithSnapshotting(10, 1),
			testContainerRedis.WithLogLevel(testContainerRedis.LogLevelVerbose),
		)

		if err != nil {
			return
		}

		// Terminate the container if an error occurs
		defer func(error2 error) {
			if error2 != nil {
				if error3 := conn.Terminate(ctx); error3 != nil {
					err = error3
					return
				}
				return
			}
		}(err)

		connectionString, err = conn.ConnectionString(ctx)
		if err != nil {
			return
		}

		parsedURL, err = url.Parse(connectionString)
		if err != nil {
			return
		}

		redisClient = redis.NewClient(&redis.Options{
			Addr: strings.Replace(parsedURL.Host, "[::1]", "127.0.0.1", 1),
			DB:   0,
		})

		if err = redisClient.Ping(ctx).Err(); err != nil {
			return
		}
	})
	return redisClient, nil
}

// newOutboxRedisRepoInstance returns a new instance of RedisStore.
func newOutboxRedisRepoInstance() (IStore, error) {
	client, err := newRedisTestContainerClient()
	if err != nil {
		return nil, err
	}
	return NewOutboxRedisRepository(Setting{
		TableName: "outbox",
	}, client), nil
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

	repo := NewOutboxRedisRepository(Setting{
		TableName: "outbox",
	}, client)

	assert.NotNil(t, repo)
}
