package redis

import (
	"ai-gateway/internal/domain"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type Cache struct {
	Client *redis.Client
	Key    string
}

func (c *Cache) PutRouting(ctx context.Context, version uint64, routes []domain.Route) error {
	b, err := json.Marshal(map[string]any{"version": version, "routes": routes})
	if err != nil {
		return err
	}
	return c.Client.Set(ctx, c.Key, b, 0).Err()
}
func (c *Cache) Invalidate(ctx context.Context) error {
	return c.Client.Incr(ctx, fmt.Sprintf("%s:version", c.Key)).Err()
}
