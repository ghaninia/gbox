package store

import (
	"context"
	"encoding/json"
	"github.com/ghaninia/gbox/dto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

type MockRepo struct {
	mock.Mock
}

func (m *MockRepo) GetTableName() string {
	return "outbox"
}

func (m *MockRepo) NewRecords(ctx context.Context, records []Outbox) error {
	args := m.Called(ctx, records)
	return args.Error(0)
}

func TestNewStore_DefaultSettings(t *testing.T) {
	repo := new(MockRepo)
	s := NewStore(repo).(*Store)

	assert.Equal(t, Setting{
		BatchInsertEnable: true,
		BatchInsertSize:   100,
		MessageBufferSize: 1000,
		TickerTimeout:     5 * time.Second,
	}, s.setting)
}

func TestDispatch_MessageInChannel(t *testing.T) {
	repo := new(MockRepo)
	s := NewStore(repo).(*Store)
	ctx := context.Background()

	msg := dto.NewMessage{Payload: "test message"}

	err := s.Dispatch(ctx, msg)
	assert.NoError(t, err)

	select {
	case outboxMsg := <-s.messageChan:
		jMsg, _ := json.Marshal(msg)
		assert.Equal(t, string(jMsg), outboxMsg.Payload)
	default:
		t.Fatal("message not added to channel")
	}
}

func TestDispatch_DirectDBInsert(t *testing.T) {
	repo := new(MockRepo)
	setting := Setting{
		DriverName:        "test.one",
		BatchInsertEnable: false,
	}

	s := NewStore(repo, setting)
	ctx := context.Background()

	msg := dto.NewMessage{Payload: "db insert test"}
	jMsg, _ := json.Marshal(msg)

	paramsMatcher := mock.MatchedBy(func(os []Outbox) bool {
		for _, o := range os {
			return o.Payload == string(jMsg) &&
				o.DriverName == setting.DriverName &&
				o.State == OutboxStatePending
		}
		return false
	})

	repo.On("NewRecords", ctx, paramsMatcher).Return(nil)
	err := s.Dispatch(ctx, msg)
	assert.NoError(t, err)
}

func TestDispatch_DirectDBInsert_WithError(t *testing.T) {
	repo := new(MockRepo)
	setting := Setting{
		DriverName:        "test.one",
		BatchInsertEnable: false,
	}

	s := NewStore(repo, setting)
	ctx := context.Background()
	msg := dto.NewMessage{Payload: "test message"}

	repo.On("NewRecords", ctx, mock.Anything).Return(assert.AnError)
	err := s.Dispatch(ctx, msg)
	assert.Error(t, err)
}

func TestDispatch_BatchInsert(t *testing.T) {
	repo := new(MockRepo)
	setting := Setting{
		DriverName:        "test.one",
		BatchInsertEnable: true,
		BatchInsertSize:   2,
		TickerTimeout:     1 * time.Second,
	}

	s := NewStore(repo, setting)
	ctx := context.Background()

	msg1 := dto.NewMessage{Payload: "db insert test 1"}
	msg2 := dto.NewMessage{Payload: "db insert test 2"}
	jMsg1, _ := json.Marshal(msg1)
	jMsg2, _ := json.Marshal(msg2)

	paramsMatcher := mock.MatchedBy(func(os []Outbox) bool {
		for _, o := range os {
			return (o.Payload == string(jMsg1) || o.Payload == string(jMsg2)) &&
				o.DriverName == setting.DriverName &&
				o.State == OutboxStatePending
		}
		return false
	})

	repo.On("NewRecords", ctx, paramsMatcher).Return(nil)
	err := s.Dispatch(ctx, msg1)
	assert.NoError(t, err)

	err = s.Dispatch(ctx, msg2)
	assert.NoError(t, err)
}
