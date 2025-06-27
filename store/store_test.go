package store

import (
	"context"
	"github.com/ghaninia/gbox/dto"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockRepository is a mock implementation of IRepository
type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) GetTableName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockRepository) NewRecords(ctx context.Context, records []dto.Outbox) error {
	args := m.Called(ctx, records)
	return args.Error(0)
}

func newTestMessage(payload string) dto.NewMessage {
	return dto.NewMessage{
		Payload: payload,
	}
}

func setupStoreWithMockRepo(t *testing.T, maxBatchSize int, interval time.Duration) (*Store, *MockRepository) {
	mockRepo := &MockRepository{}
	s := NewStore(mockRepo, Setting{
		MaxBatchSize:   maxBatchSize,
		IntervalTicker: interval,
	}).(*Store)
	return s, mockRepo
}

func TestAdd_UnderBatchSize_NoSave(t *testing.T) {
	s, mockRepo := setupStoreWithMockRepo(t, 3, time.Second)
	mockRepo.AssertNotCalled(t, "NewRecords")

	err := s.Add(context.TODO(), "test-driver", newTestMessage("msg1"))
	assert.NoError(t, err)

	assert.Len(t, s.Messages(), 1)
}

func TestAdd_ExactBatchSize_ShouldSave(t *testing.T) {
	s, mockRepo := setupStoreWithMockRepo(t, 2, time.Second)

	mockRepo.On("NewRecords", mock.Anything, mock.Anything).Return(nil).Once()

	err := s.Add(context.TODO(), "test-driver", newTestMessage("msg1"), newTestMessage("msg2"))
	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	assert.Len(t, s.Messages(), 0)
}
