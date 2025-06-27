package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ghaninia/gbox/dto"
	"github.com/ghaninia/gbox/store"
	_ "github.com/lib/pq" // PostgreSQL driver
	"log"
	"time"
)

func main() {

	ctx := context.Background()

	db, err := sql.Open("postgres", "user=gbox dbname=gbox password=gbox host=localhost port=5432 sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	log.Println("Connected to the database successfully")

	settings := store.Setting{
		NodeID:         1,
		DriverName:     "message_broker",
		MaxBatchSize:   5,
		IntervalTicker: 1 * time.Minute,
	}

	repo := store.NewOutboxSqlRepository(store.RepoSetting{
		TableName: "outbox",
	}, db)

	storeInstance := store.NewStore(repo, settings)

	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 1"})
	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 2"})
	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 3"})
	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 4"})
	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 5"})
	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 6"})
	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 7"})
	_ = storeInstance.Add(ctx, settings.DriverName, dto.NewMessage{Payload: "Lorem ipsum 8"})

	if err := storeInstance.AutoCommit(ctx); err != nil {
		log.Fatalf("Failed to commit messages: %v", err)
	}

	fmt.Println("Done autocommit")
}
