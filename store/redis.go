package store

import (
	"context"
	"encoding/json"
	"github.com/ghaninia/gbox/dto"

	"github.com/redis/go-redis/v9"
)

const (
	ttlKeyExpired = -1
)

type outboxRedisRepository struct {
	instance *redis.Client
	setting  RepoSetting
}

func NewOutboxRedisRepository(setting RepoSetting, instance *redis.Client) IRepository {
	return &outboxRedisRepository{
		instance: instance,
		setting:  setting,
	}
}

// GetTableName get a key name for redis table
func (o outboxRedisRepository) GetTableName() string {
	return o.setting.TableName
}

// NewRecords insert new records to outbox table
func (o outboxRedisRepository) NewRecords(ctx context.Context, records []dto.Outbox) error {

	// Start transaction to insert multiple records
	if _, err := o.instance.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, record := range records {
			jRecord, err := json.Marshal(record)
			if err != nil {
				return err
			}

			if err := pipe.HSet(ctx, o.GetTableName(), record.ID, string(jRecord)).Err(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Set expiration for the key to -1
	if err := o.instance.Expire(ctx, o.GetTableName(), ttlKeyExpired).Err(); err != nil {
		return err
	}

	return nil
}
