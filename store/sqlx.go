package store

import (
	"context"
	"database/sql"
	"fmt"
)

type outboxSqlxRepository struct {
	instance *sql.DB
	setting  Setting
}

func NewOutboxSqlxRepository(setting Setting, instance *sql.DB) IStore {
	return &outboxSqlxRepository{
		instance: instance,
		setting:  setting,
	}
}

// GetTableName get a table name
func (o outboxSqlxRepository) GetTableName() string {
	return o.setting.TableName
}

// NewRecords insert new records to outbox table
func (o outboxSqlxRepository) NewRecords(ctx context.Context, records []Outbox) error {
	tx, err := o.instance.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (id, payload, driver_name, state, locked_at, locked_by, last_attempted_at, number_of_attempts, error) VALUES ", o.GetTableName())
	var args []interface{}
	for i, record := range records {
		query += "(?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			record.ID,
			record.Payload,
			record.DriverName,
			record.State,
			record.LockedAt,
			record.LockedBy,
			record.LastAttemptedAt,
			record.NumberOfAttempts,
			record.Error)
		if i < len(records)-1 {
			query += ", "
		}
	}

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return tx.Rollback()
	}

	return tx.Commit()
}
