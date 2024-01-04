package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tikv/client-go/v2/tikv"
)

var _ kvStore = (*tikvStore)(nil)
var _ kvStore = (*redisStore)(nil)

type kv struct {
	key   string
	value []byte
}
type kvStore interface {
	saveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error
	saveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error
	deleteKeyKV(ctx context.Context, key string) error
	readKeyKV(ctx context.Context, key string) ([]byte, error)
	keysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error)
}

type tikvStore struct {
	client *tikv.KVStore
}

func RegisterTikvStore(client *tikv.KVStore) {
	globalKVClient = &tikvStore{
		client: client,
	}
}

func (t *tikvStore) txn(ctx context.Context, f func(*tikv.KVTxn) error) error {
	tx, err := t.client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(error)
			if ok {
				err = fe
			} else {
				err = fmt.Errorf("tikv client txn func error: %v", r)
			}
		}
	}()
	if err = f(tx); err != nil {
		return err
	}
	if !tx.IsReadOnly() {
		tx.SetEnable1PC(true)
		tx.SetEnableAsyncCommit(true)
		err = tx.Commit(context.Background())
	}
	return err

}

func (t *tikvStore) saveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return t.txn(timeoutCtx, func(tx *tikv.KVTxn) error {
		return tx.Set([]byte(key), data)
	})

}
func (t *tikvStore) saveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error {
	return t.saveKeyKVWithTTL(ctx, key, data, opts[0].ttl)
}
func (t *tikvStore) deleteKeyKV(ctx context.Context, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return t.txn(timeoutCtx, func(tx *tikv.KVTxn) error {
		return tx.Delete([]byte(key))
	})
}
func (t *tikvStore) readKeyKV(ctx context.Context, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	var (
		data []byte
		err  error
	)
	err = t.txn(timeoutCtx, func(tx *tikv.KVTxn) error {
		data, err = tx.Get(timeoutCtx, []byte(key))
		if err != nil {
			return err
		}
		return nil
	})
	return data, err
}
func (t *tikvStore) keysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error) {
	var keys = make([]kv, 0)
	err := t.txn(ctx, func(tx *tikv.KVTxn) error {
		iter, err := tx.Iter([]byte(prefix), nil)
		if err != nil {
			return err
		}
		for iter.Valid() {
			if keysOnly {
				keys = append(keys, kv{
					key:   string(iter.Key()),
					value: nil,
				})
				continue
			}
			keys = append(keys, kv{
				key:   string(iter.Key()),
				value: iter.Value(),
			})
			iter.Next()
		}
		return nil
	})
	return keys, err
}

type redisStore struct {
	client redis.UniversalClient
}

func RegisterRedisStore(client redis.UniversalClient) {
	globalKVClient = &redisStore{
		client: client,
	}
}

func (r *redisStore) saveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return r.client.Set(timeoutCtx, key, data, time.Duration(ttl)).Err()
}
func (r *redisStore) saveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	if len(opts) > 0 {
		return r.saveKeyKVWithTTL(ctx, key, data, opts[0].ttl)
	}
	return r.client.Set(timeoutCtx, key, data, 0).Err()
}
func (r *redisStore) deleteKeyKV(ctx context.Context, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	return r.client.Del(timeoutCtx, key).Err()
}
func (r *redisStore) readKeyKV(ctx context.Context, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return r.client.Get(timeoutCtx, key).Bytes()
}
func (r *redisStore) keysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error) {
	var keys = make([]kv, 0)
	iter := r.client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		if keysOnly {
			keys = append(keys, kv{
				key:   iter.Val(),
				value: nil,
			})
			continue
		}
		date, err := r.client.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			continue
		}
		keys = append(keys, kv{
			key:   iter.Val(),
			value: date,
		})
	}
	return keys, nil
}
