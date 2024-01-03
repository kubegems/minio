package cmd

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func saveKeyRedisWithTTL(ctx context.Context, client redis.UniversalClient, key string, data []byte, ttl int64) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return client.Set(timeoutCtx, key, data, time.Duration(ttl)).Err()
}

func saveKeyRedis(ctx context.Context, client redis.UniversalClient, key string, data []byte, opts ...options) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	if len(opts) > 0 {
		return saveKeyRedisWithTTL(ctx, client, key, data, opts[0].ttl)
	}
	return client.Set(timeoutCtx, key, data, 0).Err()
}

func deleteKeyRedis(ctx context.Context, client redis.UniversalClient, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	return client.Del(timeoutCtx, key).Err()
}

func readKeyRedis(ctx context.Context, client redis.UniversalClient, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return client.Get(timeoutCtx, key).Bytes()
}

type redisKV struct {
	key   string
	value []byte
}

func keysPrefixRedis(ctx context.Context, client redis.UniversalClient, prefix string, keysOnly bool) ([]redisKV, error) {
	var keys = make([]redisKV, 0)
	iter := client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		if keysOnly {
			keys = append(keys, redisKV{
				key:   iter.Val(),
				value: nil,
			})
			continue
		}
		date, err := client.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			continue
		}
		keys = append(keys, redisKV{
			key:   iter.Val(),
			value: date,
		})
	}
	return keys, nil
}
