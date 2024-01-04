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
)

// IAMKVStore - 使用redis/tikv存储iam数据信息
type IAMKVStore struct {
	sync.RWMutex
	*iamCache
	usersSysType UsersSysType
	rdb          KvStore
}

func newIAMKVStore(rdb KvStore, usersSysType UsersSysType) *IAMKVStore {
	return &IAMKVStore{
		iamCache:     newIamCache(),
		usersSysType: usersSysType,
		rdb:          rdb,
	}
}

func (rd *IAMKVStore) rlock() *iamCache {
	rd.RLock()
	return rd.iamCache
}
func (rd *IAMKVStore) runlock() {
	rd.RUnlock()
}

func (rd *IAMKVStore) lock() *iamCache {
	rd.Lock()
	return rd.iamCache
}
func (rd *IAMKVStore) unlock() {
	rd.Unlock()
}
func (rd *IAMKVStore) getUsersSysType() UsersSysType {
	return rd.usersSysType
}

func (rd *IAMKVStore) saveIAMConfig(ctx context.Context, item interface{}, itemPath string, opts ...options) error {
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
	return rd.rdb.SaveKeyKV(ctx, itemPath, data, opts...)
}

func (rd *IAMKVStore) loadIAMConfig(ctx context.Context, item interface{}, path string) error {
	data, err := rd.rdb.ReadKeyKV(ctx, path)
	if err != nil {
		return err
	}
	return getIAMConfig(item, data, path)
}

func (rd *IAMKVStore) loadIAMConfigBytes(ctx context.Context, path string) ([]byte, error) {
	data, err := rd.rdb.ReadKeyKV(ctx, path)
	if err != nil {
		return nil, err
	}
	return decryptData(data, path)
}

func (rd *IAMKVStore) deleteIAMConfig(ctx context.Context, path string) error {
	return rd.rdb.DeleteKeyKV(ctx, path)
}

func (rd *IAMKVStore) migrateUsersConfigToV1(ctx context.Context) error {
	basePrefix := iamConfigUsersPrefix
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := rd.rdb.KeysPrefixKV(ctx, basePrefix, true)
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
			rd.rdb.DeleteKeyKV(ctx, oldPolicyPath)
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

func (rd *IAMKVStore) migrateToV1(ctx context.Context) error {
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

func (rd *IAMKVStore) migrateBackendFormat(ctx context.Context) error {
	rd.Lock()
	defer rd.Unlock()
	return rd.migrateToV1(ctx)
}

func (rd *IAMKVStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
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

func (rd *IAMKVStore) getPolicyDocKV(ctx context.Context, kvs kv, m map[string]PolicyDoc) error {
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

func (rd *IAMKVStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	//  Retrieve all keys and values to avoid too many calls to etcd in case of
	//  a large number of policies
	r, err := rd.rdb.KeysPrefixKV(ctx, iamConfigPoliciesPrefix, false)
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
func (rd *IAMKVStore) addUser(ctx context.Context, user string, userType IAMUserType, u UserIdentity, m map[string]auth.Credentials) error {
	if u.Credentials.IsExpired() {
		// Delete expired identity.
		rd.rdb.DeleteKeyKV(ctx, getUserIdentityPath(user, userType))
		rd.rdb.DeleteKeyKV(ctx, getMappedPolicyPath(user, userType, false))
		return nil
	}
	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}
	m[user] = u.Credentials
	return nil
}
func (rd *IAMKVStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error {
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

func (rd *IAMKVStore) getUserKV(ctx context.Context, userkv kv, userType IAMUserType, m map[string]auth.Credentials, basePrefix string) error {
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

func (rd *IAMKVStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
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
	r, err := rd.rdb.KeysPrefixKV(cctx, basePrefix, false)
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
func (rd *IAMKVStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
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

func redisKvsToSet(prefix string, kvs []kv) set.StringSet {
	users := set.NewStringSet()
	for _, kv := range kvs {
		user := extractPathPrefixAndSuffix(string(kv.key), prefix, path.Base(string(kv.key)))
		users.Add(user)
	}
	return users
}

func (rd *IAMKVStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := rd.rdb.KeysPrefixKV(cctx, iamConfigGroupsPrefix, true)
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
func (rd *IAMKVStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
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
func getRedisMappedPolicy(ctx context.Context, kv kv, userType IAMUserType, isGroup bool, m map[string]MappedPolicy, basePrefix string) error {
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

func (rd *IAMKVStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
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
	r, err := rd.rdb.KeysPrefixKV(cctx, basePrefix, false)
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

func (rd *IAMKVStore) savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error {
	return rd.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}
func (rd *IAMKVStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return rd.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}
func (rd *IAMKVStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	return rd.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}
func (rd *IAMKVStore) saveGroupInfo(ctx context.Context, group string, gi GroupInfo) error {
	return rd.saveIAMConfig(ctx, gi, getGroupInfoPath(group))
}
func (rd *IAMKVStore) deletePolicyDoc(ctx context.Context, policyName string) error {
	err := rd.deleteIAMConfig(ctx, getPolicyDocPath(policyName))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}
func (rd *IAMKVStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := rd.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}
func (rd *IAMKVStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	err := rd.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}
func (rd *IAMKVStore) deleteGroupInfo(ctx context.Context, name string) error {
	err := rd.deleteIAMConfig(ctx, getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}
