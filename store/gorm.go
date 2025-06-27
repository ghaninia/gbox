package store

import (
	"context"
	"github.com/ghaninia/gbox/dto"

	"gorm.io/gorm"
)

type outboxGormRepository struct {
	instance *gorm.DB
	setting  RepoSetting
}

func NewOutboxGormRepository(setting RepoSetting, instance *gorm.DB) IRepository {
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
func (o outboxGormRepository) NewRecords(ctx context.Context, records []dto.Outbox) error {
	return o.instance.WithContext(ctx).
		Table(o.GetTableName()).
		Create(records).Error
}
