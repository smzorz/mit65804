package shardgrp

import (
	crand "crypto/rand"
	"log"
	"math/big"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ckDebug       = false
	grpRPCTimeout = 750 * time.Millisecond
)

func ckLog(format string, a ...interface{}) {
	if ckDebug {
		log.Printf("[Clerk] "+format, a...)
	}
}

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
	seqNum   int64
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	return bigx.Int64()
}
func init() {
	rand.Seed(time.Now().UnixNano())
}

func (ck *Clerk) advanceLeader() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	backoff := 50*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond
	time.Sleep(backoff)
}
func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leaderId = 0
	ck.seqNum = 0
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) nextSeqNum() int64 {
	ck.seqNum++
	return ck.seqNum
}

// NextSeqNum exposes the next operation sequence number so callers can
// reuse it across retries (e.g., upper-layer clerks coordinating multiple
// attempts of the same logical Put).
func (ck *Clerk) NextSeqNum() int64 {
	return ck.nextSeqNum()
}

func (ck *Clerk) callWithTimeout(server, method string, args interface{}, reply interface{}) (bool, bool) {
	done := make(chan bool, 1)
	go func() {
		done <- ck.clnt.Call(server, method, args, reply)
	}()
	select {
	case ok := <-done:
		return ok, false
	case <-time.After(grpRPCTimeout):
		return false, true
	}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	ck.seqNum++
	failedAttempts := 0
	for {

		args := &rpc.GetArgs{Key: key, SeqNum: ck.seqNum, ClientId: ck.clientId}
		ckLog("Client: %d Get: %+v", ck.clientId, args)
		var reply rpc.GetReply
		ok, timedOut := ck.callWithTimeout(ck.servers[ck.leaderId], "KVServer.Get", args, &reply)
		if timedOut {
			ckLog("Client: %d Get: timed out", ck.clientId)
			failedAttempts++
			if failedAttempts >= len(ck.servers) {
				return "", 0, rpc.ErrMaybe
			}
			ck.advanceLeader()
			continue
		}
		if ok {
			failedAttempts = 0
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				ckLog("Client: %d Get: reply: %+v", ck.clientId, reply)
				return reply.Value, reply.Version, reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				ckLog("Client: %d Get: wrong leader", ck.clientId)
				ck.advanceLeader()
				continue
			}
			if reply.Err == rpc.ErrWrongGroup {
				ckLog("Client: %d Get: wrong group", ck.clientId)
				return "", 0, rpc.ErrWrongGroup
			}
		} else {
			ckLog("Client: %d Get: failed to call", ck.clientId)
			ckLog("servers: %v", ck.servers)
			ckLog("leaderId: %d", ck.leaderId)
			ckLog("Put: failed to call")
			failedAttempts++
			if failedAttempts >= len(ck.servers) {
				return "", 0, rpc.ErrMaybe
			}
		}
		ck.advanceLeader()
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	return ck.putWithSeq(key, value, version, 0)
}

func (ck *Clerk) PutWithSeq(key string, value string, version rpc.Tversion, seqNum int64) rpc.Err {
	return ck.putWithSeq(key, value, version, seqNum)
}

func (ck *Clerk) putWithSeq(key string, value string, version rpc.Tversion, seqNum int64) rpc.Err {
	retried := false
	failedAttempts := 0
	opSeq := seqNum
	if opSeq == 0 {
		opSeq = ck.nextSeqNum()
	}
	for {
		args := &rpc.PutArgs{Key: key, Value: value, Version: version, SeqNum: opSeq, ClientId: ck.clientId}
		ckLog("Put: %+v", args)
		var reply rpc.PutReply
		ok, timedOut := ck.callWithTimeout(ck.servers[ck.leaderId], "KVServer.Put", args, &reply)
		if timedOut {
			retried = true
			failedAttempts++
			if failedAttempts >= len(ck.servers) {
				return rpc.ErrMaybe
			}
			ck.advanceLeader()
			continue
		}
		if ok {
			failedAttempts = 0
			ckLog("Put: reply: %+v", reply)
			switch reply.Err {
			case rpc.OK:
				ckLog("Put: reply: %+v", reply)
				return rpc.OK
			case rpc.ErrVersion:
				ckLog("Put: version error")

				if retried {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			case rpc.ErrWrongLeader:

				ckLog("Put: wrong leader")
				// rotate below
				retried = true
			case rpc.ErrNoKey:
				ckLog("Put: no key error")
				if retried {
					return rpc.ErrMaybe
				}
				return rpc.ErrNoKey
			case rpc.ErrWrongGroup:
				ckLog("Put: wrong group")
				return rpc.ErrWrongGroup
			default:
				ckLog("Put: other error: %v", reply.Err)
				if retried {
					return rpc.ErrMaybe
				}
				return reply.Err
			}
		} else {
			ckLog("servers: %v", ck.servers)
			ckLog("leaderId: %d", ck.leaderId)
			ckLog("Put: failed to call")
			retried = true
			failedAttempts++
			if failedAttempts >= len(ck.servers) {
				return rpc.ErrMaybe
			}
		}
		ck.advanceLeader()
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	ck.seqNum++
	args := &shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	failedAttempts := 0
	maxAttempts := len(ck.servers)
	if maxAttempts == 0 {
		return nil, rpc.ErrMaybe
	}
	for {
		var reply shardrpc.FreezeShardReply
		ckLog("FreezeShard: %+v", args)
		ok, timedOut := ck.callWithTimeout(ck.servers[ck.leaderId], "KVServer.FreezeShard", args, &reply)
		if timedOut {
			failedAttempts++
			if failedAttempts >= maxAttempts {
				return nil, rpc.ErrMaybe
			}
			ck.advanceLeader()
			continue
		}
		if ok {
			failedAttempts = 0
			ckLog("FreezeShard: reply: %+v", reply)
			switch reply.Err {
			case rpc.OK:
				ckLog("FreezeShard: reply: %+v", reply)
				return reply.State, rpc.OK
			case rpc.ErrWrongLeader:
				ck.advanceLeader()
				continue
			case rpc.ErrWrongGroup:
				return nil, rpc.ErrWrongGroup
			default:
				failedAttempts++
				if failedAttempts >= maxAttempts {
					return nil, rpc.ErrMaybe
				}
				ck.advanceLeader()
				continue
			}
		} else {
			failedAttempts++
			if failedAttempts >= maxAttempts {
				return nil, rpc.ErrMaybe
			}
			ck.advanceLeader()
		}

	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	ck.seqNum++
	args := &shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	failedAttempts := 0
	maxAttempts := len(ck.servers)
	if maxAttempts == 0 {
		return rpc.ErrMaybe
	}
	for {
		var reply shardrpc.InstallShardReply
		ckLog("InstallShard: %+v", args)
		ok, timedOut := ck.callWithTimeout(ck.servers[ck.leaderId], "KVServer.InstallShard", args, &reply)
		if timedOut {
			failedAttempts++
			if failedAttempts >= maxAttempts {
				return rpc.ErrMaybe
			}
			ck.advanceLeader()
			continue
		}
		if ok {
			failedAttempts = 0
			ckLog("InstallShard: reply: %+v", reply)
			switch reply.Err {
			case rpc.OK:
				ckLog("InstallShard: reply: %+v", reply)
				return rpc.OK
			case rpc.ErrWrongLeader:
				ck.advanceLeader()
				continue
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			default:
				failedAttempts++
				if failedAttempts >= maxAttempts {
					return rpc.ErrMaybe
				}
				ck.advanceLeader()
				continue
			}
		} else {
			failedAttempts++
			if failedAttempts >= maxAttempts {
				return rpc.ErrMaybe
			}
			ck.advanceLeader()
		}

	}

}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	ck.seqNum++
	args := &shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	failedAttempts := 0
	maxAttempts := len(ck.servers)
	if maxAttempts == 0 {
		return rpc.ErrMaybe
	}
	for {
		var reply shardrpc.DeleteShardReply
		ckLog("DeleteShard: %+v", args)
		ok, timedOut := ck.callWithTimeout(ck.servers[ck.leaderId], "KVServer.DeleteShard", args, &reply)
		if timedOut {
			failedAttempts++
			if failedAttempts >= maxAttempts {
				return rpc.ErrMaybe
			}
			ck.advanceLeader()
			continue
		}
		if ok {
			failedAttempts = 0
			ckLog("DeleteShard: reply: %+v", reply)
			switch reply.Err {
			case rpc.OK:
				ckLog("DeleteShard: reply: %+v", reply)
				return rpc.OK
			case rpc.ErrWrongLeader:
				ck.advanceLeader()
				continue
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			default:
				failedAttempts++
				if failedAttempts >= maxAttempts {
					return rpc.ErrMaybe
				}
				ck.advanceLeader()
				continue
			}
		} else {
			failedAttempts++
			if failedAttempts >= maxAttempts {
				return rpc.ErrMaybe
			}
			ck.advanceLeader()
		}

	}
}
