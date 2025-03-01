package store

import (
	"context"
	"log"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/postgres"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	testContainerPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	testContainerRedis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
)

var (
	onceDB     sync.Once
	sqlxClient *sqlx.DB
	gormClient *gorm.DB
)

var (
	redisClient *redis.Client
	onceRedis   sync.Once
)

// seeders returns the SQL queries to seed the database.
func seeders() []string {
	return []string{
		`CREATE TABLE outbox (
			id SERIAL PRIMARY KEY,
			payload TEXT NOT NULL,
			driver_name VARCHAR(255) NOT NULL,
			state VARCHAR(255) NOT NULL,
			created_at TIMESTAMP NOT NULL,
			locked_at TIMESTAMP,
			locked_by VARCHAR(255),
			last_attempted_at TIMESTAMP,
			number_of_attempts INTEGER,
			error TEXT
		);`,
	}
}

// newDBTestContainerClient returns a new instance of SQLX and GORM clients.
func newDBTestContainerClient() (_ *sqlx.DB, _ *gorm.DB, err error) {
	onceDB.Do(func() {
		var (
			ctx              = context.Background()
			dbName           = "outbox"
			dbUserName       = "outbox"
			dbPassword       = "password"
			conn             *testContainerPostgres.PostgresContainer
			connectionString string
		)

		conn, err = testContainerPostgres.Run(ctx,
			"postgres:16.2",
			testContainerPostgres.WithDatabase(dbName),
			testContainerPostgres.WithUsername(dbUserName),
			testContainerPostgres.WithPassword(dbPassword),
			testcontainers.WithWaitStrategy(
				wait.
					ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(5*time.Second)),
		)

		if err != nil {
			return
		}

		// Terminate the container if an error occurs
		connectionString, err = conn.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			return
		}

		sqlxClient, err = sqlx.Open("postgres", connectionString)
		if err != nil {
			return
		}

		for _, query := range seeders() {
			_, err = sqlxClient.Exec(query)
			if err != nil {
				return
			}
		}

		gormClient, err = gorm.Open(
			postgres.Open(connectionString), &gorm.Config{},
		)

		if err != nil {
			return
		}
	})
	return sqlxClient, gormClient, nil
}

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

// TestMain is the entry point for the test suite.
func TestMain(m *testing.M) {

	log.Printf("starting [store folder] test suite ...")

	sqlxClient, gormClient, err := newDBTestContainerClient()
	if err != nil {
		panic(err)
	}

	if sqlxClient == nil || gormClient == nil {
		panic("sqlxClient or gormClient is nil")
	}

	redisClient, err = newRedisTestContainerClient()
	if err != nil {
		panic(err)
	}

	if redisClient == nil {
		panic("redisClient is nil")
	}

	m.Run()

	log.Printf("stopping [store folder] test suite ...")
}
