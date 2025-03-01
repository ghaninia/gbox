package store

import (
	"context"
	"database/sql"
	"fmt"
)

type outboxSqlRepository struct {
	instance *sql.DB
	setting  Setting
}

func NewOutboxSqlRepository(setting Setting, instance *sql.DB) IStore {
	return &outboxSqlRepository{
		instance: instance,
		setting:  setting,
	}
}

func (o outboxSqlRepository) GetTableName() string {
	return o.setting.TableName
}

func (o outboxSqlRepository) NewRecords(ctx context.Context, records []Outbox) error {

	tx, err := o.instance.Begin()
	if err != nil {
		return err
	}

	statement := fmt.Sprintf("INSERT INTO %s (id, payload, driver_name, state, created_at, locked_at, locked_by, last_attempted_at, number_of_attempts, error) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", o.GetTableName())
	stmt, err := tx.PrepareContext(ctx, statement)

	if err != nil {
		return tx.Rollback()
	}

	for _, record := range records {
		if _, err = stmt.ExecContext(
			ctx,
			record.ID,
			record.Payload,
			record.DriverName,
			record.State,
			record.CreatedAt,
			record.LockedAt,
			record.LockedBy,
			record.LastAttemptedAt,
			record.NumberOfAttempts,
			record.Error); err != nil {
			return tx.Rollback()
		}
	}

	return tx.Commit()
}
