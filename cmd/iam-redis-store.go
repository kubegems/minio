package cmd

import (
	"context"
	"encoding/json"
	"path"
	"sync"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
	"github.com/redis/go-redis/v9"
)

// IAMRedisStore - 使用redis存储iam数据信息
type IAMRedisStore struct {
	sync.RWMutex
	*iamCache
	usersSysType UsersSysType
	rdb          redis.UniversalClient
}

func newIAMRedisStore(rdb redis.UniversalClient, usersSysType UsersSysType) *IAMRedisStore {
	return &IAMRedisStore{
		iamCache:     newIamCache(),
		usersSysType: usersSysType,
		rdb:          rdb,
	}
}

func (rd *IAMRedisStore) rlock() *iamCache {
	rd.RLock()
	return rd.iamCache
}
func (rd *IAMRedisStore) runlock() {
	rd.RUnlock()
}

func (rd *IAMRedisStore) lock() *iamCache {
	rd.Lock()
	return rd.iamCache
}
func (rd *IAMRedisStore) unlock() {
	rd.Unlock()
}
func (rd *IAMRedisStore) getUsersSysType() UsersSysType {
	return rd.usersSysType
}

func (rd *IAMRedisStore) saveIAMConfig(ctx context.Context, item interface{}, itemPath string, opts ...options) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if GlobalKMS != nil {
		data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, itemPath),
		})
		if err != nil {
			return err
		}
	}
	return saveKeyRedis(ctx, rd.rdb, itemPath, data, opts...)
}

func (rd *IAMRedisStore) loadIAMConfig(ctx context.Context, item interface{}, path string) error {
	data, err := readKeyRedis(ctx, rd.rdb, path)
	if err != nil {
		return err
	}
	return getIAMConfig(item, data, path)
}

func (rd *IAMRedisStore) loadIAMConfigBytes(ctx context.Context, path string) ([]byte, error) {
	data, err := readKeyRedis(ctx, rd.rdb, path)
	if err != nil {
		return nil, err
	}
	return decryptData(data, path)
}

func (rd *IAMRedisStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteKeyRedis(ctx, rd.rdb, path)
}

func (rd *IAMRedisStore) migrateUsersConfigToV1(ctx context.Context) error {
	basePrefix := iamConfigUsersPrefix
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := keysPrefixRedis(ctx, rd.rdb, basePrefix, true)
	if err != nil {
		return err
	}

	users := redisKvsToSet(basePrefix, r)
	for _, user := range users.ToSlice() {
		{
			// 1. check if there is a policy file in the old loc.
			oldPolicyPath := pathJoin(basePrefix, user, iamPolicyFile)
			var policyName string
			err := rd.loadIAMConfig(ctx, &policyName, oldPolicyPath)
			if err != nil {
				switch err {
				case errConfigNotFound:
					// No mapped policy or already migrated.
				default:
					// corrupt data/read error, etc
				}
				goto next
			}

			// 2. copy policy to new loc.
			mp := newMappedPolicy(policyName)
			userType := regUser
			path := getMappedPolicyPath(user, userType, false)
			if err := rd.saveIAMConfig(ctx, mp, path); err != nil {
				return err
			}

			// 3. delete policy file in old loc.
			deleteKeyRedis(ctx, rd.rdb, oldPolicyPath)
		}

	next:
		// 4. check if user identity has old format.
		identityPath := pathJoin(basePrefix, user, iamIdentityFile)
		var cred auth.Credentials
		if err := rd.loadIAMConfig(ctx, &cred, identityPath); err != nil {
			switch err {
			case errConfigNotFound:
				// This case should not happen.
			default:
				// corrupt file or read error
			}
			continue
		}

		// If the file is already in the new format,
		// then the parsed auth.Credentials will have
		// the zero value for the struct.
		var zeroCred auth.Credentials
		if cred.Equal(zeroCred) {
			// nothing to do
			continue
		}

		// Found a id file in old format. Copy value
		// into new format and save it.
		cred.AccessKey = user
		u := newUserIdentity(cred)
		if err := rd.saveIAMConfig(ctx, u, identityPath); err != nil {
			return err
		}

		// Nothing to delete as identity file location
		// has not changed.
	}
	return nil
}

func (rd *IAMRedisStore) migrateToV1(ctx context.Context) error {
	var iamFmt iamFormat
	path := getIAMFormatFilePath()
	if err := rd.loadIAMConfig(ctx, &iamFmt, path); err != nil {
		switch err {
		case errConfigNotFound:
			// Need to migrate to V1.
		default:
			// if IAM format
			return err
		}
	}

	if iamFmt.Version >= iamFormatVersion1 {
		// Nothing to do.
		return nil
	}

	if err := rd.migrateUsersConfigToV1(ctx); err != nil {
		return err
	}

	// Save iam format to version 1.
	if err := rd.saveIAMConfig(ctx, newIAMFormatVersion1(), path); err != nil {
		return err
	}

	return nil
}

func (rd *IAMRedisStore) migrateBackendFormat(ctx context.Context) error {
	rd.Lock()
	defer rd.Unlock()
	return rd.migrateToV1(ctx)
}

func (rd *IAMRedisStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
	data, err := rd.loadIAMConfigBytes(ctx, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}

	var p PolicyDoc
	err = p.parseJSON(data)
	if err != nil {
		return err
	}

	m[policy] = p
	return nil
}

func (rd *IAMRedisStore) getPolicyDocKV(ctx context.Context, kvs redisKV, m map[string]PolicyDoc) error {
	data, err := decryptData(kvs.value, string(kvs.key))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}

	var p PolicyDoc
	err = p.parseJSON(data)
	if err != nil {
		return err
	}

	policy := extractPathPrefixAndSuffix(string(kvs.key), iamConfigPoliciesPrefix, path.Base(string(kvs.key)))
	m[policy] = p
	return nil
}

func (rd *IAMRedisStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	//  Retrieve all keys and values to avoid too many calls to etcd in case of
	//  a large number of policies
	r, err := keysPrefixRedis(ctx, rd.rdb, iamConfigPoliciesPrefix, false)
	if err != nil {
		return err
	}
	// Parse all values to construct the policies data model.
	for _, kvs := range r {
		if err = rd.getPolicyDocKV(ctx, kvs, m); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}
func (rd *IAMRedisStore) addUser(ctx context.Context, user string, userType IAMUserType, u UserIdentity, m map[string]auth.Credentials) error {
	if u.Credentials.IsExpired() {
		// Delete expired identity.
		deleteKeyRedis(ctx, rd.rdb, getUserIdentityPath(user, userType))
		deleteKeyRedis(ctx, rd.rdb, getMappedPolicyPath(user, userType, false))
		return nil
	}
	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}
	m[user] = u.Credentials
	return nil
}
func (rd *IAMRedisStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error {
	var u UserIdentity
	err := rd.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}
	return rd.addUser(ctx, user, userType, u, m)
}

func (rd *IAMRedisStore) getUserKV(ctx context.Context, userkv redisKV, userType IAMUserType, m map[string]auth.Credentials, basePrefix string) error {
	var u UserIdentity
	err := getIAMConfig(&u, userkv.value, string(userkv.key))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}
	user := extractPathPrefixAndSuffix(string(userkv.key), basePrefix, path.Base(string(userkv.key)))
	return rd.addUser(ctx, user, userType, u, m)
}

func (rd *IAMRedisStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
	var basePrefix string
	switch userType {
	case svcUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	// Retrieve all keys and values to avoid too many calls to etcd in case of
	// a large number of users
	r, err := keysPrefixRedis(cctx, rd.rdb, basePrefix, false)
	if err != nil {
		return err
	}

	// Parse all users values to create the proper data model
	for _, userKv := range r {
		if err = rd.getUserKV(ctx, userKv, userType, m, basePrefix); err != nil && err != errNoSuchUser {
			return err
		}
	}
	return nil
}
func (rd *IAMRedisStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
	var gi GroupInfo
	err := rd.loadIAMConfig(ctx, &gi, getGroupInfoPath(group))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchGroup
		}
		return err
	}
	m[group] = gi
	return nil
}

func redisKvsToSet(prefix string, kvs []redisKV) set.StringSet {
	users := set.NewStringSet()
	for _, kv := range kvs {
		user := extractPathPrefixAndSuffix(string(kv.key), prefix, path.Base(string(kv.key)))
		users.Add(user)
	}
	return users
}

func (rd *IAMRedisStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := keysPrefixRedis(cctx, rd.rdb, iamConfigGroupsPrefix, true)
	if err != nil {
		return err
	}

	groups := redisKvsToSet(iamConfigGroupsPrefix, r)

	// Reload config for all groups.
	for _, group := range groups.ToSlice() {
		if err = rd.loadGroup(ctx, group, m); err != nil && err != errNoSuchGroup {
			return err
		}
	}
	return nil
}
func (rd *IAMRedisStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	var p MappedPolicy
	err := rd.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil
}
func getRedisMappedPolicy(ctx context.Context, kv redisKV, userType IAMUserType, isGroup bool, m map[string]MappedPolicy, basePrefix string) error {
	var p MappedPolicy
	err := getIAMConfig(&p, kv.value, string(kv.key))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	name := extractPathPrefixAndSuffix(string(kv.key), basePrefix, ".json")
	m[name] = p
	return nil
}

func (rd *IAMRedisStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	var basePrefix string
	if isGroup {
		basePrefix = iamConfigPolicyDBGroupsPrefix
	} else {
		switch userType {
		case svcUser:
			basePrefix = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePrefix = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePrefix = iamConfigPolicyDBUsersPrefix
		}
	}
	// Retrieve all keys and values to avoid too many calls to etcd in case of
	// a large number of policy mappings
	r, err := keysPrefixRedis(cctx, rd.rdb, basePrefix, false)
	if err != nil {
		return err
	}

	// Parse all policies mapping to create the proper data model
	for _, kv := range r {
		if err = getRedisMappedPolicy(ctx, kv, userType, isGroup, m, basePrefix); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

func (rd *IAMRedisStore) savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error {
	return rd.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}
func (rd *IAMRedisStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return rd.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}
func (rd *IAMRedisStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	return rd.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}
func (rd *IAMRedisStore) saveGroupInfo(ctx context.Context, group string, gi GroupInfo) error {
	return rd.saveIAMConfig(ctx, gi, getGroupInfoPath(group))
}
func (rd *IAMRedisStore) deletePolicyDoc(ctx context.Context, policyName string) error {
	err := rd.deleteIAMConfig(ctx, getPolicyDocPath(policyName))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}
func (rd *IAMRedisStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := rd.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}
func (rd *IAMRedisStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	err := rd.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}
func (rd *IAMRedisStore) deleteGroupInfo(ctx context.Context, name string) error {
	err := rd.deleteIAMConfig(ctx, getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}
