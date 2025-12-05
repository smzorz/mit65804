package shardctrler

import (
	"log"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const ScDebug = false // 建议跑测试时设为 false 以减少日志干扰

func ScLog(format string, a ...interface{}) {
	if !ScDebug {
		return
	}
	log.Printf("[ShardCtrler] "+format, a...)
}

type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	killed int32
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

// InitController 保持你的逻辑基本不变，它是正确的
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

	// 只有当 Intent(Next) > Current 时才恢复
	if nexCfg.Num > curCfg.Num {
		ScLog("Recovering config change from %d to %d", curCfg.Num, nexCfg.Num)

		// Phase 1: Move
		var moved []shardcfg.Tshid
		var ok bool
		for {
			moved, ok = sck.transferShardsNoDelete(curCfg, nexCfg)
			if ok {
				break
			}
			sck.backoff()
		}

		// Phase 2: Commit
		for {
			putErr := sck.IKVClerk.Put("config", nexCfg.String(), rpc.Tversion(curCfg.Num))
			if putErr == rpc.OK || putErr == rpc.ErrVersion {
				break
			}
			sck.backoff()
		}

		// Phase 3: Cleanup
		for _, shard := range moved {
			srcGid := curCfg.Shards[shard]
			srcServers := curCfg.Groups[srcGid]
			srcClerk := sck.makeGroupClerk(srcServers)
			for {
				if srcClerk == nil {
					sck.backoff()
					srcClerk = sck.makeGroupClerk(srcServers)
					continue
				}
				err := srcClerk.DeleteShard(shard, curCfg.Num)
				if err == rpc.OK || err == rpc.ErrWrongGroup {
					break
				}
				ScLog("Recovery DeleteShard failed shard %d: %v", shard, err)
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

		// Phase 0: Write Intent (WAL)
		if !sck.persistNextConfig(new) {
			sck.backoff()
			continue
		}

		// Phase 1: Move
		moved, ok := sck.transferShardsNoDelete(oldCfg, new)
		if !ok {
			ScLog("transferShardsNoDelete failed, retrying...")
			sck.backoff()
			continue
		}
		ScLog("transferShardsNoDelete succeeded, moved %d shards", len(moved))

		// Phase 2: Commit
		putErr := sck.IKVClerk.Put("config", new.String(), rpc.Tversion(oldCfg.Num))
		ScLog("Phase 2 commit: putErr=%v, oldNum=%d, newNum=%d", putErr, oldCfg.Num, new.Num)

		if putErr == rpc.OK {
			// Clear the intent log after successful commit
			sck.IKVClerk.Put("newconfig", "", 0)
			ScLog("Config committed successfully, cleared newconfig")

			// Phase 3 cleanup happens asynchronously so we don't block config change completion.
			oldSnapshot := oldCfg.Copy()
			go sck.cleanupTransferredShards(oldSnapshot, moved)
			ScLog("ChangeConfigTo returning after committing num %d", new.Num)
			return
		}
		if putErr != rpc.ErrVersion {
			ScLog("Put failed: %v", putErr)
			sck.backoff()
		}
	}
}

func (sck *ShardCtrler) persistNextConfig(next *shardcfg.ShardConfig) bool {
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
					return true
				}
			}

			putErr = sck.IKVClerk.Put("newconfig", next.String(), ver)
		}

		switch putErr {
		case rpc.OK:
			return true
		// ErrVersion 意味着并发写入，循环重试
		case rpc.ErrVersion, rpc.ErrMaybe:
			sck.backoff()
			continue
		default:
			sck.backoff()
		}
	}
}

func (sck *ShardCtrler) transferShardsNoDelete(oldCfg, newCfg *shardcfg.ShardConfig) ([]shardcfg.Tshid, bool) {

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
	for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
		srcGid := oldCfg.Shards[shard]
		dstGid := newCfg.Shards[shard]
		if srcGid == dstGid || dstGid == 0 || srcGid == 0 {
			continue
		}
		srcServers, ok := oldCfg.Groups[srcGid]
		if !ok {
			ScLog("transferShardsNoDelete: missing srcGid %d", srcGid)
			return nil, false
		}
		dstServers, ok := newCfg.Groups[dstGid]
		if !ok {
			ScLog("transferShardsNoDelete: missing dstGid %d", dstGid)
			return nil, false
		}

		var state []byte
		skipShard := false
		srcClerk := getClerk(srcClerks, srcGid, srcServers)
		for {
			ScLog("FreezeShard: shard %d, num %d from gid %d", shard, oldCfg.Num, srcGid)
			st, err := srcClerk.FreezeShard(shard, oldCfg.Num)
			if err == rpc.OK {
				state = st
				break
			}
			if err == rpc.ErrWrongGroup {
				ScLog("FreezeShard: ErrWrongGroup for shard %d (already moved)", shard)
				skipShard = true
				break
			}
			ScLog("FreezeShard retry shard %d err=%v", shard, err)
			sck.backoff()
		}

		if skipShard {
			continue
		}

		dstClerk := getClerk(dstClerks, dstGid, dstServers)
		for {
			ScLog("InstallShard: shard %d, num %d to gid %d", shard, newCfg.Num, dstGid)
			err := dstClerk.InstallShard(shard, state, newCfg.Num)
			if err == rpc.OK {
				break
			}
			if err == rpc.ErrWrongGroup {
				ScLog("InstallShard: ErrWrongGroup for shard %d (already installed)", shard)
				break
			}
			ScLog("InstallShard retry shard %d err=%v", shard, err)
			sck.backoff()
		}
		moved = append(moved, shard)
	}
	return moved, true
}

// cleanupTransferredShards best-effort deletes frozen shards from the old groups.
func (sck *ShardCtrler) cleanupTransferredShards(oldCfg *shardcfg.ShardConfig, moved []shardcfg.Tshid) {
	for _, shard := range moved {
		srcGid := oldCfg.Shards[shard]
		srcServers := oldCfg.Groups[srcGid]
		for {
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
			sck.backoff()
		}
	}
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
