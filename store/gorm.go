package store

import (
	"context"

	"gorm.io/gorm"
)

type outboxGormRepository struct {
	instance *gorm.DB
	setting  Setting
}

func NewOutboxGormRepository(setting Setting, instance *gorm.DB) IStore {
	return &outboxGormRepository{
		instance: instance,
		setting:  setting,
	}
}

// GetTableName get a table name
func (o outboxGormRepository) GetTableName() string {
	return o.setting.TableName
}

// NewRecords insert new records to outbox table
func (o outboxGormRepository) NewRecords(ctx context.Context, records []Outbox) error {
	return o.instance.WithContext(ctx).
		Table(o.GetTableName()).
		Create(records).Error
}
