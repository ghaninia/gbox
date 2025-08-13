package poller

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ghaninia/gbox/constant"
	"github.com/ghaninia/gbox/dto"
	"github.com/ghaninia/gbox/store"
)

type IWorker interface {
	Start(ctx context.Context) error
	Stop()
}

// WorkerConfig defines the configuration for each worker.
type WorkerConfig struct {
	BatchSizeProcessing int           `default:"100"`
	TimeoutPerMessage   time.Duration `default:"5s"`
	DelayWhenNoMessages time.Duration `default:"1s"`
}

type worker struct {
	// DI attributes
	providers IProviders
	store     store.IStore

	// inside worker attributes
	sync.Mutex
	inProgressMessages []dto.Outbox
	gracefulStop       bool
	stopped            bool
	workerID           int

	// config worker attributes
	cfg WorkerConfig
}

func newWorker(
	providers IProviders,
	store store.IStore,
	workerID int,
	cfg WorkerConfig,
) IWorker {
	return &worker{
		providers: providers,
		store:     store,
		workerID:  workerID,
		cfg:       cfg,
	}
}

func (w *worker) Start(ctx context.Context) error {
	log.Printf("[Worker %d] started", w.workerID)

	for {
		// Check if worker is stopped gracefully or has been requested to stop
		// This is to ensure that the worker can exit cleanly when no messages are left to process.
		if w.gracefulStop {
			w.Lock()
			noWork := len(w.inProgressMessages) == 0
			w.Unlock()
			if noWork {
				log.Printf("[Worker %d] graceful stop completed", w.workerID)
				w.stopped = true
				return nil
			}
		}

		select {
		case <-ctx.Done():
			log.Printf("[Worker %d] context canceled, stopping immediately", w.workerID)
			w.stopped = true
			return nil
		default:

			// Fetch a batch of messages
			messages, err := w.store.FetchMessages(ctx, w.cfg.BatchSizeProcessing)
			if err != nil {
				log.Printf("[Worker %d] fetch error: %v", w.workerID, err)
				time.Sleep(w.cfg.DelayWhenNoMessages)
				continue
			}

			// If no messages are fetched, wait for a while before retrying
			// This helps to avoid busy-waiting and allows other workers to process messages.
			if len(messages) == 0 {
				time.Sleep(w.cfg.DelayWhenNoMessages)
				continue
			}

			// Set in-progress messages for this worker
			w.Lock()
			w.inProgressMessages = append([]dto.Outbox(nil), messages...)
			w.Unlock()

			// Process messages
			for _, msg := range messages {

				// Check if worker should stop gracefully
				if w.gracefulStop && ctx.Err() == nil {
					break
				}

				// Timeout per message processing
				msgCtx, cancel := context.WithTimeout(ctx, w.cfg.TimeoutPerMessage)
				err := w.processMessage(msgCtx, msg)
				cancel()

				if err != nil {
					log.Printf("[Worker %d] failed to process msg %s: %v", w.workerID, msg.ID, err)
					continue
				}

				// Acknowledge the message as processed
				if err := w.store.MarkAsProcessed(ctx, msg.ID); err != nil {
					log.Printf("[Worker %d] failed to ack msg %s: %v", w.workerID, msg.ID, err)
				}

				// Remove from in-progress list
				w.Lock()
				for i, inProg := range w.inProgressMessages {
					if inProg.ID == msg.ID {
						w.inProgressMessages = append(w.inProgressMessages[:i], w.inProgressMessages[i+1:]...)
						break
					}
				}
				w.Unlock()
			}
		}
	}
}

// processMessage processes a single message using the appropriate provider.
// DLQ Or Retry logic can be implemented here if needed.
func (w *worker) processMessage(ctx context.Context, msg dto.Outbox) error {
	provider := w.providers.GetProvider(msg.DriverName)
	if provider == nil {
		return constant.ErrProviderNotFound
	}
	return provider.Handle(ctx, msg)
}

func (w *worker) Stop() {
	log.Printf("[Worker %d] graceful stop requested", w.workerID)
	w.gracefulStop = true
}
