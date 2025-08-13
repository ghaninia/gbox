package poller

import (
	"context"
	"sync"

	"github.com/ghaninia/gbox/dto"
)

type IProvider interface {
	DriverName() string
	Handle(ctx context.Context, record dto.Outbox) error
}

type IProviders interface {
	AddProvider(provider IProvider) IProviders
	Providers() []IProvider
	GetProvider(name string) IProvider
	Handle(ctx context.Context, record dto.Outbox) error
}

type providers struct {
	sync.RWMutex
	providers map[string]IProvider
}

func NewProviders() IProviders {
	return &providers{
		providers: make(map[string]IProvider),
	}
}

// AddProvider adds a new provider to the collection.
func (p *providers) AddProvider(provider IProvider) IProviders {
	p.Lock()
	defer p.Unlock()
	p.providers[provider.DriverName()] = provider
	return p
}

// Providers returns a slice of all registered providers.
func (p *providers) Providers() []IProvider {
	var listWithoutKey []IProvider
	for _, provider := range p.providers {
		listWithoutKey = append(listWithoutKey, provider)
	}
	return listWithoutKey
}

// GetProvider retrieves a provider by its name.
func (p *providers) GetProvider(name string) IProvider {
	p.Lock()
	defer p.Unlock()
	if provider, exists := p.providers[name]; exists {
		return provider
	}
	return nil
}

// Handle processes a record using the appropriate provider based on the driver's name.
func (p *providers) Handle(ctx context.Context, record dto.Outbox) error {
	provider := p.GetProvider(record.DriverName)
	if provider == nil {
		return nil
	}
	return provider.Handle(ctx, record)
}
