package poller

import (
	"context"

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
	providers map[string]IProvider
}

func NewProviders() IProviders {
	return &providers{
		providers: make(map[string]IProvider),
	}
}

// AddProvider adds a new provider to the collection.
func (p *providers) AddProvider(provider IProvider) IProviders {
	p.providers[provider.DriverName()] = provider
	return p
}

// Providers returns a slice of all registered providers.
func (p *providers) Providers() []IProvider {
	var list []IProvider
	for _, provider := range p.providers {
		list = append(list, provider)
	}
	return list
}

// GetProvider retrieves a provider by its name.
func (p *providers) GetProvider(name string) IProvider {
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
