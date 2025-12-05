package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

var upCkDebug = false

func upCkLog(format string, a ...interface{}) {
	if upCkDebug {
		log.Printf("[uPClerk] "+format, a...)
	}
}

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
	config      shardcfg.ShardConfig
	groupClerks map[tester.Tgid]*groupClient
}

type groupClient struct {
	clerk   *shardgrp.Clerk
	servers []string
}

const errRetryPause = 2 * time.Millisecond

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:        clnt,
		sck:         sck,
		groupClerks: make(map[tester.Tgid]*groupClient),
	}
	// You'll have to add code here.
	ck.refreshConfig()
	return ck
}

func (ck *Clerk) refreshConfig() {
	if cfg := ck.sck.Query(); cfg != nil {
		if cfg.Num >= ck.config.Num {
			ck.config = *cfg
		}
	}
}

func (ck *Clerk) getGroupInfo(key string) (tester.Tgid, []string) {
	shard := shardcfg.Key2Shard(key)
	gid := ck.config.Shards[shard]
	servers := ck.config.Groups[gid]
	return gid, servers
}

func sameServers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func copyServers(src []string) []string {
	if len(src) == 0 {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func (ck *Clerk) shardgrpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	if gid == 0 || len(servers) == 0 {
		return nil
	}
	if gc, ok := ck.groupClerks[gid]; ok {
		if sameServers(gc.servers, servers) {
			return gc.clerk
		}
	}
	sgck := shardgrp.MakeClerk(ck.clnt, servers)
	ck.groupClerks[gid] = &groupClient{clerk: sgck, servers: copyServers(servers)}
	return sgck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	ck.refreshConfig()
	for {
		gid, servers := ck.getGroupInfo(key)
		if len(servers) == 0 {
			ck.refreshConfig()
			time.Sleep(errRetryPause)
			continue
		}
		sgck := ck.shardgrpClerk(gid, servers)
		if sgck == nil {
			ck.refreshConfig()
			time.Sleep(errRetryPause)
			continue
		}
		val, ver, err := sgck.Get(key)
		switch err {
		case rpc.OK:
			upCkLog("Get: key %s value %s version %d", key, val, ver)
			return val, ver, rpc.OK
		case rpc.ErrWrongGroup:
			upCkLog("Get: wrong group")
			delete(ck.groupClerks, gid)
			ck.refreshConfig()
			time.Sleep(errRetryPause)

			continue
		case rpc.ErrMaybe:
			upCkLog("Get: maybe error")
			ck.refreshConfig()
			time.Sleep(errRetryPause)
			continue
		case rpc.ErrNoKey:
			upCkLog("Get: no key error")
			return "", 0, rpc.ErrNoKey
		default:
			return "", 0, err
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	ck.refreshConfig()
	var currentSeq int64
	var currentClerk *shardgrp.Clerk
	for {
		gid, servers := ck.getGroupInfo(key)
		if len(servers) == 0 {
			ck.refreshConfig()
			time.Sleep(errRetryPause)
			continue
		}
		sgck := ck.shardgrpClerk(gid, servers)
		if sgck == nil {
			ck.refreshConfig()
			time.Sleep(errRetryPause)
			continue
		}
		if currentClerk != sgck || currentSeq == 0 {
			currentClerk = sgck
			currentSeq = sgck.NextSeqNum()
		}
		err := sgck.PutWithSeq(key, value, version, currentSeq)
		switch err {

		case rpc.OK:
			return rpc.OK
		case rpc.ErrWrongGroup:
			// the group has changed, so remove the old clerk
			upCkLog("Put: wrong group")
			delete(ck.groupClerks, gid)
			currentClerk = nil
			currentSeq = 0
			ck.refreshConfig()
			time.Sleep(errRetryPause)

			continue
		case rpc.ErrMaybe:
			upCkLog("Put: maybe error")
			ck.refreshConfig()
			time.Sleep(errRetryPause)

			continue

		default:
			upCkLog("Put: other error: %v", err)

			return err
		}
	}
}
