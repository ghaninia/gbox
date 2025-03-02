package main

import (
	"context"
	"fmt"
	"github.com/ghaninia/gbox/dto"
	"github.com/ghaninia/gbox/store"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"time"
)

func main() {

	connectionString := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=disable", "gbox", "postgres", "gbox", "localhost", "5432")

	sqlxClient, err := sqlx.Open("postgres", connectionString)
	if err != nil {
		panic("failed to connect database")
	}

	//gormClient, err = gorm.Open(
	//	postgres.Open(connectionString), &gorm.Config{},
	//)
	//if err != nil {
	//	return
	//}
	//
	//sqlClient, err = sql.Open("postgres", connectionString)
	//if err != nil {
	//	return
	//}

	repo := store.NewOutboxSqlxRepository(store.RepoSetting{
		TableName: "outbox",
	}, sqlxClient)

	s := store.NewStore(repo, store.Setting{
		DriverName:        "post.like",
		BatchInsertEnable: true,
		BatchInsertSize:   2,
		MessageBufferSize: 10,
		TickerTimeout:     5 * time.Second,
	})

	_ = s.Dispatch(context.Background(), dto.NewMessage{
		Payload: "message 01",
	})

	_ = s.Dispatch(context.Background(), dto.NewMessage{
		Payload: "message 02",
	})

	_ = s.Dispatch(context.Background(), dto.NewMessage{
		Payload: "message 03",
	})

	_ = s.Dispatch(context.Background(), dto.NewMessage{
		Payload: "message 04",
	})

	_ = s.Dispatch(context.Background(), dto.NewMessage{
		Payload: "message 05",
	})

	defer s.Close()
}
