package store

import (
	"context"
	"github.com/ghaninia/gbox/dto"
	"github.com/stretchr/testify/mock"
)

type MockRepo struct {
	mock.Mock
}

func (m *MockRepo) GetTableName() string {
	return "outbox"
}

func (m *MockRepo) NewRecords(ctx context.Context, records []dto.Outbox) error {
	args := m.Called(ctx, records)
	return args.Error(0)
}
