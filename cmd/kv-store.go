package cmd

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	jlog "github.com/juicedata/juicefs/pkg/utils"
	"github.com/redis/go-redis/v9"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
)

var _ KvStore = (*TikvStore)(nil)
var _ KvStore = (*RedisStore)(nil)
var kvLogger = jlog.GetLogger("juicefs")

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
	Name() string
}

type TikvStore struct {
	nsMutex *nsLockMap
	client  *txnkv.Client
	prefix  []byte
}

func RegisterTikvStore(client *txnkv.Client, prefix []byte) {
	GlobalKVClient = &TikvStore{
		client:  client,
		nsMutex: newNSLock(false),
		prefix:  prefix,
	}
}

func (t *TikvStore) realKey(key string) []byte {
	bt := []byte(key)
	k := make([]byte, len(t.prefix)+len(bt))
	copy(k, t.prefix)
	copy(k[len(t.prefix):], bt)
	return k
}

func (t *TikvStore) originKey(key []byte) string {
	return string(key[len(t.prefix):])
}

func (t *TikvStore) Name() string {
	return "tikv"
}

func (t *TikvStore) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error) {
	kvLogger.Infof("GetObjectNInfo: bucket: %s, object: %s", bucket, object)
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}
	nsUnlocker := func() {}
	if lockType != noLock {
		// Lock the object before reading.
		lock := t.nsMutex.NewNSLock(nil, bucket, object)
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
	data, err = t.ReadKeyKV(ctx, pathJoin(bucket, object))
	if err != nil {
		nsUnlocker()
		if tikverr.IsErrNotFound(err) {
			return nil, ObjectNotFound{Bucket: bucket, Object: object}
		}
		return nil, err
	}
	return NewGetObjectReaderFromReader(bytes.NewBuffer(data), ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, opts, nsUnlocker)
}

func (t *TikvStore) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	kvLogger.Infof("PutObject: bucket: %s, object: %s", bucket, object)
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return ObjectInfo{}, err
	}
	lk := t.nsMutex.NewNSLock(nil, bucket, object)
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
	}, t.SaveKeyKV(ctx, pathJoin(bucket, object), rd)
}
func (t *TikvStore) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	kvLogger.Infof("DeleteObject: bucket: %s, object: %s", bucket, object)
	lk := t.nsMutex.NewNSLock(nil, bucket, object)
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
	}, t.DeleteKeyKV(ctx, pathJoin(bucket, object))
}

func (t *TikvStore) SaveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	tx, err := t.client.Begin()
	if err != nil {
		return err
	}
	err = tx.Set(t.realKey(key), data)
	if err != nil {
		return err
	}
	return tx.Commit(timeoutCtx)
}
func (t *TikvStore) SaveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error {
	if len(opts) > 0 {
		return t.SaveKeyKVWithTTL(ctx, key, data, opts[0].ttl)
	}
	return t.SaveKeyKVWithTTL(ctx, key, data, 0)
}
func (t *TikvStore) DeleteKeyKV(ctx context.Context, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	tx, err := t.client.Begin()
	if err != nil {
		return err
	}
	err = tx.Delete(t.realKey(key))
	if err != nil {
		return err
	}
	return tx.Commit(timeoutCtx)
}
func (t *TikvStore) ReadKeyKV(ctx context.Context, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	tx, err := t.client.Begin()
	if err != nil {
		return nil, err
	}
	data, err := tx.Get(timeoutCtx, t.realKey(key))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (t *TikvStore) KeysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error) {
	tx, err := t.client.Begin()
	if err != nil {
		return nil, err
	}
	iter, err := tx.Iter(t.realKey(prefix), nextKey(t.realKey(prefix)))
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var keys = make([]kv, 0)
	for iter.Valid() {
		keys = append(keys, kv{
			key:   t.originKey(iter.Key()[:]),
			value: iter.Value()[:],
		})
		if err = iter.Next(); err != nil {
			break
		}
	}
	return keys, nil
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

func (r *RedisStore) Name() string {
	return "redis"
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
