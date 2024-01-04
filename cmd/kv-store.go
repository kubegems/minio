package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tikv/client-go/v2/tikv"
)

var _ KvStore = (*TikvStore)(nil)
var _ KvStore = (*RedisStore)(nil)

type kv struct {
	key   string
	value []byte
}
type KvStore interface {
	ObjectIO
	SaveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error
	SaveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error
	DeleteKeyKV(ctx context.Context, key string) error
	ReadKeyKV(ctx context.Context, key string) ([]byte, error)
	KeysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error)
}

type TikvStore struct {
	nsMutex *nsLockMap
	client  *tikv.KVStore
}

func RegisterTikvStore(client *tikv.KVStore) {
	GlobalKVClient = &TikvStore{
		client:  client,
		nsMutex: newNSLock(false),
	}
}

func (t *TikvStore) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error) {
	return
}

func (t *TikvStore) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return
}
func (t *TikvStore) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, nil
}

func (t *TikvStore) txn(ctx context.Context, f func(*tikv.KVTxn) error) error {
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

func (t *TikvStore) SaveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return t.txn(timeoutCtx, func(tx *tikv.KVTxn) error {
		return tx.Set([]byte(key), data)
	})

}
func (t *TikvStore) SaveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error {
	return t.SaveKeyKVWithTTL(ctx, key, data, opts[0].ttl)
}
func (t *TikvStore) DeleteKeyKV(ctx context.Context, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return t.txn(timeoutCtx, func(tx *tikv.KVTxn) error {
		return tx.Delete([]byte(key))
	})
}
func (t *TikvStore) ReadKeyKV(ctx context.Context, key string) ([]byte, error) {
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
func (t *TikvStore) KeysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error) {
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

type RedisStore struct {
	nsMutex *nsLockMap
	client  redis.UniversalClient
}

func RegisterRedisStore(client redis.UniversalClient) {
	GlobalKVClient = &RedisStore{
		client:  client,
		nsMutex: newNSLock(false),
	}
}

func (r *RedisStore) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}
	nsUnlocker := func() {}
	if lockType != noLock {
		// Lock the object before reading.
		lock := r.nsMutex.NewNSLock(nil, bucket, object)
		switch lockType {
		case writeLock:
			lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.Unlock(lkctx.Cancel) }
		case readLock:
			lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.RUnlock(lkctx.Cancel) }
		}
	}
	var data []byte
	data, err = r.ReadKeyKV(ctx, pathJoin(bucket, object))
	if err != nil {
		nsUnlocker()
		if err == redis.Nil {
			return nil, ObjectNotFound{Bucket: bucket, Object: object}
		}
		return nil, err
	}
	return NewGetObjectReaderFromReader(bytes.NewBuffer(data), ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, opts, nsUnlocker)
}

func (r *RedisStore) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return ObjectInfo{}, err
	}
	lk := r.nsMutex.NewNSLock(nil, bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return objInfo, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)
	rd, err := io.ReadAll(data.Reader)
	if err != nil {
		return objInfo, err
	}
	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, r.SaveKeyKV(ctx, pathJoin(bucket, object), rd)
}

func (r *RedisStore) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	lk := r.nsMutex.NewNSLock(nil, bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)
	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, r.DeleteKeyKV(ctx, pathJoin(bucket, object))
}

func (r *RedisStore) SaveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return r.client.Set(timeoutCtx, key, data, time.Duration(ttl)).Err()
}
func (r *RedisStore) SaveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	if len(opts) > 0 {
		return r.SaveKeyKVWithTTL(ctx, key, data, opts[0].ttl)
	}
	return r.client.Set(timeoutCtx, key, data, 0).Err()
}
func (r *RedisStore) DeleteKeyKV(ctx context.Context, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	return r.client.Del(timeoutCtx, key).Err()
}
func (r *RedisStore) ReadKeyKV(ctx context.Context, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	return r.client.Get(timeoutCtx, key).Bytes()
}
func (r *RedisStore) KeysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error) {
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
