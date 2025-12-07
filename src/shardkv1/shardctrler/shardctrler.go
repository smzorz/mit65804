package shardctrler

import (
	"fmt"
	"log"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	ScDebug = false
)

func ScLog(format string, a ...interface{}) {
	if !ScDebug {
		return
	}
	log.Printf("[ShardCtrler] "+format, a...)
}

type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
}

func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

func (sck *ShardCtrler) makeGroupClerk(servers []string) *shardgrp.Clerk {
	return shardgrp.MakeClerk(sck.clnt, servers)
}

func (sck *ShardCtrler) InitController() {
	curVal, _, _ := sck.IKVClerk.Get("config")
	var curCfg *shardcfg.ShardConfig
	if curVal == "" {
		curCfg = &shardcfg.ShardConfig{Num: 0}
	} else {
		curCfg = shardcfg.FromString(curVal)
	}

	nexVal, _, _ := sck.IKVClerk.Get("newconfig")
	if nexVal == "" {
		return
	}
	nexCfg := shardcfg.FromString(nexVal)

	if nexCfg.Num >= curCfg.Num {
		ScLog("Recovering config change from %d to %d", curCfg.Num, nexCfg.Num)

		var moved []shardcfg.Tshid
		var ok bool
		for {
			var advanced bool
			moved, ok, advanced = sck.transferShardsNoDelete(curCfg, nexCfg)
			if advanced {
				ScLog("InitController aborting recovery for config %d: observed newer config", nexCfg.Num)
				sck.clearNewConfigIntent(nexCfg)
				return
			}
			if ok {
				break
			}
			sck.backoff()
		}

		for {
			putErr := sck.IKVClerk.Put("config", nexCfg.String(), rpc.Tversion(curCfg.Num))
			if putErr == rpc.OK || putErr == rpc.ErrVersion {
				// config is committed (either by us or someone else). Persist
				// the committed config for debugging and clear the intent so
				// future controllers don't keep retrying the same transition.
				sck.saveConfigHistory(nexCfg)
				sck.clearNewConfigIntent(nexCfg)
				break
			}
			sck.backoff()
		}

		for _, shard := range moved {
			srcGid := curCfg.Shards[shard]
			srcServers := curCfg.Groups[srcGid]
			for {
				if len(srcServers) == 0 {
					ScLog("Recovery DeleteShard skip shard %d: no servers for gid %d", shard, srcGid)
					break
				}
				srcClerk := sck.makeGroupClerk(srcServers)
				if srcClerk == nil {
					sck.backoff()
					continue
				}
				err := srcClerk.DeleteShard(shard, curCfg.Num)
				if err == rpc.OK || err == rpc.ErrWrongGroup {
					break
				}
				ScLog("Recovery DeleteShard failed shard %d: %v", shard, err)
				if err == rpc.ErrMaybe {
					if cur := sck.currentConfig(); cur != nil {
						ScLog("config:%v", cur)
						if updated, ok := cur.Groups[srcGid]; ok {
							srcServers = updated
							ScLog("servers %v", srcServers)
						}
					}
				}
				sck.backoff()
			}
		}
	}
}

func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	val := cfg.String()
	sck.IKVClerk.Put("config", val, 0)
}

func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		oldCfg := sck.currentConfig()
		if oldCfg == nil {
			sck.backoff()
			continue
		}
		if new.Num <= oldCfg.Num {
			return
		}
		if new.Num != oldCfg.Num+1 {
			sck.backoff()
			continue
		}

		oldSnapshot := oldCfg.Copy()

		// persistNextConfig now returns (ok, conflict). If there's an
		// intent conflict (same Num but different content), abort
		// this ChangeConfigTo attempt to avoid executing against a
		// config that wasn't actually persisted by us.
		ok, conflict := sck.persistNextConfig(new)
		if conflict {
			ScLog("ChangeConfigTo aborting num %d: intent conflict detected for newconfig", new.Num)
			sck.clearNewConfigIntent(new)
			return
		}
		if !ok {
			sck.backoff()
			continue
		}

		moved, ok, advanced := sck.transferShardsNoDelete(oldCfg, new)
		if advanced {
			ScLog("ChangeConfigTo aborting num %d: observed newer config", new.Num)
			sck.clearNewConfigIntent(new)
			return
		}
		if !ok {
			ScLog("transferShardsNoDelete failed, retrying...")
			sck.backoff()
			continue
		}
		ScLog("transferShardsNoDelete succeeded, moved %d shards,olgcfg: %v,newcfg:%v", len(moved), oldCfg, new)

	commitLoop:
		for {
			putErr := sck.IKVClerk.Put("config", new.String(), rpc.Tversion(oldCfg.Num))
			ScLog("Phase 2 commit: putErr=%v, oldNum=%d, newNum=%d", putErr, oldCfg.Num, new.Num)

			switch putErr {
			case rpc.OK:
				// Persist the committed config for debugging before clearing intent
				sck.saveConfigHistory(new)
				sck.clearNewConfigIntent(new)
				ScLog("Config committed successfully, cleared newconfig")
				go sck.cleanupTransferredShards(oldSnapshot, moved)
				ScLog("ChangeConfigTo returning after committing num %d", new.Num)
				return

			case rpc.ErrMaybe:
				cur := sck.currentConfig()
				if cur != nil && cur.Num >= new.Num {
					// Another controller likely committed; persist observed committed config
					sck.saveConfigHistory(cur)
					sck.clearNewConfigIntent(new)
					ScLog("Config likely committed (ErrMaybe, cur=%d), issuing cleanup", cur.Num)
					go sck.cleanupTransferredShards(oldSnapshot, moved)
					return
				}
				sck.backoff()
				continue commitLoop

			case rpc.ErrVersion:
				cur := sck.currentConfig()
				if cur != nil && cur.Num >= new.Num {
					// Observed newer committed config; persist for debugging
					sck.saveConfigHistory(cur)
					sck.clearNewConfigIntent(new)
					ScLog("Config observed advanced to %d, issuing cleanup", cur.Num)
					go sck.cleanupTransferredShards(oldSnapshot, moved)
					return
				}
				break commitLoop

			default:
				ScLog("Put failed: %v", putErr)
				sck.backoff()
			}
		}
	}
}

func (sck *ShardCtrler) clearNewConfigIntent(justCommitted *shardcfg.ShardConfig) {
	for {
		val, ver, err := sck.IKVClerk.Get("newconfig")
		if err != rpc.OK && err != rpc.ErrNoKey {
			sck.backoff()
			continue
		}

		if val == "" || err == rpc.ErrNoKey {
			return
		}

		existing := shardcfg.FromString(val)
		if existing.Num != justCommitted.Num {
			ScLog("Skipping cleanup: newconfig has changed (num %d != %d)", existing.Num, justCommitted.Num)
			return
		}

		err = sck.IKVClerk.Put("newconfig", "", ver)
		if err == rpc.OK {
			return
		}

		if err == rpc.ErrVersion {
			continue
		}
		sck.backoff()
	}
}

func (sck *ShardCtrler) persistNextConfig(next *shardcfg.ShardConfig) (bool, bool) {
	// returns (ok, conflict)
	for {
		val, ver, err := sck.IKVClerk.Get("newconfig")
		if err != rpc.OK && err != rpc.ErrNoKey {
			sck.backoff()
			continue
		}

		var putErr rpc.Err

		if err == rpc.ErrNoKey {
			putErr = sck.IKVClerk.Put("newconfig", next.String(), 0)
		} else {
			if val != "" {
				existing := shardcfg.FromString(val)
				if existing.Num == next.Num {
					// If the existing intent has same Num, ensure the content
					// matches exactly. If not, this is an intent conflict and
					// we must not proceed with our migration.
					if existing.String() == next.String() {
						return true, false
					}
					ScLog("persistNextConfig: intent conflict for Num %d: existing=%v next=%v", next.Num, existing, next)
					return false, true
				}
			}

			putErr = sck.IKVClerk.Put("newconfig", next.String(), ver)
		}

		switch putErr {
		case rpc.OK:
			return true, false
		case rpc.ErrVersion, rpc.ErrMaybe:
			sck.backoff()
			continue
		default:
			sck.backoff()
		}
	}
}

func (sck *ShardCtrler) transferShardsNoDelete(oldCfg, newCfg *shardcfg.ShardConfig) ([]shardcfg.Tshid, bool, bool) {

	srcClerks := make(map[tester.Tgid]*shardgrp.Clerk)
	dstClerks := make(map[tester.Tgid]*shardgrp.Clerk)
	getClerk := func(cache map[tester.Tgid]*shardgrp.Clerk, gid tester.Tgid, servers []string) *shardgrp.Clerk {
		if ck, ok := cache[gid]; ok {
			return ck
		}
		ck := sck.makeGroupClerk(servers)
		cache[gid] = ck
		return ck
	}

	moved := make([]shardcfg.Tshid, 0)

	abortDueToAdvanced := func(format string, a ...interface{}) ([]shardcfg.Tshid, bool, bool) {
		ScLog(format, a...)
		return nil, false, true
	}

	for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
		srcGid := oldCfg.Shards[shard]
		dstGid := newCfg.Shards[shard]
		// Diagnostic: log mapping decisions for each shard so we can
		// see why Freeze/Install is attempted for a shard that appears
		// unchanged between configs.
		ScLog("transferShardsNoDelete: shard %d oldNum=%d newNum=%d srcGid=%d dstGid=%d", shard, oldCfg.Num, newCfg.Num, srcGid, dstGid)
		if srcGid == dstGid || dstGid == 0 || srcGid == 0 {
			continue
		}
		srcServers, ok := oldCfg.Groups[srcGid]
		if !ok || len(srcServers) == 0 {
			ScLog("transferShardsNoDelete: missing/empty srcGid %d", srcGid)
			return nil, false, false
		}
		dstServers, ok := newCfg.Groups[dstGid]
		if !ok || len(dstServers) == 0 {
			ScLog("transferShardsNoDelete: missing/empty dstGid %d", dstGid)
			return nil, false, false
		}

		var state []byte
		skipShard := false
		srcClerk := getClerk(srcClerks, srcGid, srcServers)
	freezeLoop:
		for {
			if sck.configAdvancedSince(oldCfg.Num) {
				return abortDueToAdvanced("Aborting FreezeShard for shard %d: observed newer config", shard)
			}
			ScLog("FreezeShard: shard %d, num %d from gid %d", shard, oldCfg.Num, srcGid)
			st, err := srcClerk.FreezeShard(shard, oldCfg.Num)
			switch err {
			case rpc.OK:
				state = st
				break freezeLoop
			case rpc.ErrWrongGroup:
				ScLog("FreezeShard: ErrWrongGroup for shard %d (already moved)", shard)
				skipShard = true
				break freezeLoop
			default:
				ScLog("FreezeShard retry shard %d err=%v", shard, err)
				if sck.configAdvancedSince(oldCfg.Num) {
					return abortDueToAdvanced("Observed newer config during FreezeShard retry for shard %d", shard)
				}
				sck.backoff()
				continue freezeLoop
			}
		}

		if skipShard {
			continue
		}

		dstClerk := getClerk(dstClerks, dstGid, dstServers)
	installLoop:
		for {
			if sck.configAdvancedSince(oldCfg.Num) {
				return abortDueToAdvanced("Aborting InstallShard for shard %d: observed newer config", shard)
			}
			ScLog("InstallShard: shard %d, num %d to gid %d", shard, newCfg.Num, dstGid)
			err := dstClerk.InstallShard(shard, state, newCfg.Num)
			switch err {
			case rpc.OK:
				break installLoop
			case rpc.ErrWrongGroup:
				ScLog("InstallShard: ErrWrongGroup for shard %d (already installed)", shard)
				break installLoop
			default:
				ScLog("InstallShard retry shard %d err=%v", shard, err)
				if sck.configAdvancedSince(oldCfg.Num) {
					return abortDueToAdvanced("Observed newer config during InstallShard retry for shard %d", shard)
				}
				sck.backoff()
				continue installLoop
			}
		}
		moved = append(moved, shard)
	}

	return moved, true, false
}

func (sck *ShardCtrler) cleanupTransferredShards(oldCfg *shardcfg.ShardConfig, moved []shardcfg.Tshid) {
	for _, shard := range moved {
		srcGid := oldCfg.Shards[shard]
		srcServers := oldCfg.Groups[srcGid]
		retryCount := 0
		for {
			if len(srcServers) == 0 {
				ScLog("DeleteShard skip shard %d: no servers for gid %d", shard, srcGid)
				break
			}
			srcClerk := sck.makeGroupClerk(srcServers)
			if srcClerk == nil {
				sck.backoff()
				continue
			}
			err := srcClerk.DeleteShard(shard, oldCfg.Num)
			if err == rpc.OK || err == rpc.ErrWrongGroup {
				break
			}
			ScLog("DeleteShard rpc failed for shard %d: %v", shard, err)
			if err == rpc.ErrMaybe {
				if cur := sck.currentConfig(); cur != nil {
					if updated, ok := cur.Groups[srcGid]; ok {
						srcServers = updated
					}
				}
			}

			retryCount++
			if retryCount > 20 {
				ScLog("DeleteShard giving up after %d attempts for shard %d: last err=%v", retryCount, shard, err)
				break
			}
			sck.backoff()
		}
	}
}

// saveConfigHistory persists a copy of the provided config under the key
// `cfghist-<num>` in the backing IKV store to make historical configs easy
// to inspect for debugging.
func (sck *ShardCtrler) saveConfigHistory(cfg *shardcfg.ShardConfig) {
	if cfg == nil {
		return
	}
	key := fmt.Sprintf("cfghist-%d", cfg.Num)
	err := sck.IKVClerk.Put(key, cfg.String(), 0)
	if err != rpc.OK {
		ScLog("saveConfigHistory: Put returned %v for key %s", err, key)
	} else {
		ScLog("saveConfigHistory: saved config %d -> %s", cfg.Num, key)
	}
}

func (sck *ShardCtrler) configAdvancedSince(oldNum shardcfg.Tnum) bool {
	cur := sck.currentConfig()
	if cur == nil {
		return false
	}
	return cur.Num > oldNum
}

func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	val, _, err := sck.IKVClerk.Get("config")
	if err != rpc.OK {
		return nil
	}
	cfg := shardcfg.FromString(val)
	return cfg
}

func (sck *ShardCtrler) currentConfig() *shardcfg.ShardConfig {
	val, _, err := sck.IKVClerk.Get("config")
	if err != rpc.OK {
		return nil
	}
	if val == "" {
		cfg := shardcfg.MakeShardConfig()
		cfg.Num = 0
		return cfg
	}
	return shardcfg.FromString(val)
}

func (sck *ShardCtrler) backoff() {
	time.Sleep(50 * time.Millisecond)
}
