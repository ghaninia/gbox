package poller

import (
	"context"
	"log"
	"sync"

	"github.com/ghaninia/gbox/store"
	"golang.org/x/sync/errgroup"
)

type WorkerPoolConfig struct {
	CountOfWorkers int
	Worker         WorkerConfig
}

type workerPool struct {
	// inside worker pool attributes
	sync.Mutex
	workers []IWorker
	cancel  context.CancelFunc

	// DI attributes
	// This is to ensure that the worker pool can access the necessary providers and store.
	cfg       WorkerPoolConfig
	providers IProviders
	store     store.IStore
}

func NewWorkerPool(
	providers IProviders,
	store store.IStore,
	cfg WorkerPoolConfig,
) *workerPool {
	return &workerPool{
		providers: providers,
		store:     store,
		cfg:       cfg,
		workers:   make([]IWorker, 0, cfg.CountOfWorkers),
	}
}

func (wp *workerPool) StartBlocking(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	wp.cancel = cancel

	errGroup, gCtx := errgroup.WithContext(ctx)

	for i := 0; i < wp.cfg.CountOfWorkers; i++ {

		// Create a new worker instance for each worker in the pool.
		worker := newWorker(
			wp.providers,
			wp.store,
			i,
			wp.cfg.Worker,
		)

		wp.Lock()
		wp.workers = append(wp.workers, worker)
		wp.Unlock()

		errGroup.Go(func() error {
			return worker.Start(gCtx)
		})
	}

	return errGroup.Wait()
}

func (wp *workerPool) Stop() {
	log.Println("[Pool] graceful stop requested")
	wp.Lock()
	for _, w := range wp.workers {
		w.Stop()
	}
	wp.Unlock()
}
