package services

import (
	"ai-gateway/internal/domain"
	"context"
	"net/http"
)

// Repository boundaries keep business services independent from PostgreSQL.
type Repository interface {
	Load(ctx context.Context) ([]domain.Account, []domain.Provider, []domain.APIKey, []domain.Model, error)
	Save(ctx context.Context, accounts []domain.Account, providers []domain.Provider, keys []domain.APIKey, models []domain.Model) error
}

type Cache interface {
	PutRouting(ctx context.Context, version uint64, routes []domain.Route) error
	Invalidate(ctx context.Context) error
}

type ProviderAdapter interface {
	Name() string
	Proxy(ctx context.Context, client *http.Client, route domain.Route, payload []byte) (*http.Response, error)
}
