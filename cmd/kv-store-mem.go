package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/google/btree"
)

func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	next := make([]byte, len(key))
	copy(next, key)
	p := len(next) - 1
	for {
		next[p]++
		if next[p] != 0 {
			break
		}
		p--
		if p < 0 {
			panic("can't scan keys for 0xFF")
		}
	}
	return next
}

type kvItem struct {
	key   string
	ver   int
	value []byte
}

func (it *kvItem) Less(o btree.Item) bool {
	return it.key < o.(*kvItem).key
}

type memKV struct {
	sync.Mutex
	items *btree.BTree
	temp  *kvItem
}

func (c *memKV) shouldRetry(err error) bool {
	return strings.Contains(err.Error(), "write conflict")
}
func (c *memKV) get(key string) *kvItem {
	c.temp.key = key
	it := c.items.Get(c.temp)
	if it != nil {
		return it.(*kvItem)
	}
	return nil
}
func (c *memKV) set(key string, value []byte) {
	c.temp.key = key
	if value == nil {
		c.items.Delete(c.temp)
		return
	}
	it := c.items.Get(c.temp)
	if it != nil {
		it.(*kvItem).ver++
		it.(*kvItem).value = value
	} else {
		c.items.ReplaceOrInsert(&kvItem{key: key, ver: 1, value: value})
	}
}

func (c *memKV) scan(prefix []byte, handler func(key []byte, value []byte)) error {
	c.Lock()
	snap := c.items.Clone()
	c.Unlock()
	begin := string(prefix)
	end := string(nextKey(prefix))
	snap.AscendGreaterOrEqual(&kvItem{key: begin}, func(i btree.Item) bool {
		it := i.(*kvItem)
		if end != "" && it.key >= end {
			return false
		}
		handler([]byte(it.key), it.value)
		return true
	})
	return nil
}

var _ KvStore = (*MemStore)(nil)

type MemStore struct {
	nsMutex *nsLockMap
	client  *memKV
}

func RegisterMemStore() {
	GlobalKVClient = &MemStore{
		client: &memKV{
			items: btree.New(2),
			temp:  &kvItem{},
		},
		nsMutex: newNSLock(false),
	}
}

func (m *MemStore) Name() string {
	return "memory"
}

// DeleteKeyKV implements KvStore.
func (m *MemStore) DeleteKeyKV(ctx context.Context, key string) error {
	m.client.set(key, nil)
	return nil
}

// DeleteObject implements KvStore.
func (m *MemStore) DeleteObject(ctx context.Context, bucket string, object string, opts ObjectOptions) (ObjectInfo, error) {
	lk := m.nsMutex.NewNSLock(nil, bucket, object)
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
	}, m.DeleteKeyKV(ctx, pathJoin(bucket, object))
}

// GetObjectNInfo implements KvStore.
func (m *MemStore) GetObjectNInfo(ctx context.Context, bucket string, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}
	nsUnlocker := func() {}
	if lockType != noLock {
		// Lock the object before reading.
		lock := m.nsMutex.NewNSLock(nil, bucket, object)
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
	data, err = m.ReadKeyKV(ctx, pathJoin(bucket, object))
	if err != nil {
		nsUnlocker()
		if err == errMemNotFound {
			return nil, ObjectNotFound{Bucket: bucket, Object: object}
		}
		return nil, err
	}
	return NewGetObjectReaderFromReader(bytes.NewBuffer(data), ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, opts, nsUnlocker)
}

// KeysPrefixKV implements KvStore.
func (m *MemStore) KeysPrefixKV(ctx context.Context, prefix string, keysOnly bool) ([]kv, error) {
	var keys = make([]kv, 0)
	err := m.client.scan([]byte(prefix), func(key []byte, value []byte) {
		if keysOnly {
			keys = append(keys, kv{key: string(key)})
			return
		}
		keys = append(keys, kv{key: string(key), value: value})
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// PutObject implements KvStore.
func (m *MemStore) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return ObjectInfo{}, err
	}
	lk := m.nsMutex.NewNSLock(nil, bucket, object)
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
	}, m.SaveKeyKV(ctx, pathJoin(bucket, object), rd)
}

var errMemNotFound = errors.New("key not found")

// ReadKeyKV implements KvStore.
func (m *MemStore) ReadKeyKV(ctx context.Context, key string) ([]byte, error) {
	it := m.client.get(key)
	if it == nil {
		return nil, errMemNotFound
	}
	return it.value, nil
}

// SaveKeyKV implements KvStore.
func (m *MemStore) SaveKeyKV(ctx context.Context, key string, data []byte, opts ...options) error {
	m.client.set(key, data)
	return nil
}

// SaveKeyKVWithTTL implements KvStore.
func (m *MemStore) SaveKeyKVWithTTL(ctx context.Context, key string, data []byte, ttl int64) error {
	m.client.set(key, data)
	return nil
}
