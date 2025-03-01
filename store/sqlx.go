package store

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type outboxSqlxRepository struct {
	instance *sqlx.DB
	setting  RepoSetting
}

func NewOutboxSqlxRepository(setting RepoSetting, instance *sqlx.DB) IRepository {
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

	query := fmt.Sprintf(`INSERT INTO %s (id, payload, driver_name, state,created_at , locked_at, locked_by, last_attempted_at, number_of_attempts, error) VALUES (:id, :payload, :driver_name, :state, :created_at, :locked_at, :locked_by, :last_attempted_at, :number_of_attempts, :error)`, o.GetTableName())

	if _, err := o.instance.NamedExecContext(ctx, query, records); err != nil {
		return err
	}

	return nil
}
