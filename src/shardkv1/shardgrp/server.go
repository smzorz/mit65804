package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const kvDebug = false

func kvLog(kv *KVServer, format string, a ...interface{}) {
	if !kvDebug {
		return
	}
	log.Printf("[G%d S%d] "+format, append([]interface{}{kv.gid, kv.me}, a...)...)
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu   sync.Mutex
	data map[shardcfg.Tshid]map[string]struct {
		Value   string
		Version rpc.Tversion
	}
	lastApplied        map[shardcfg.Tshid]map[int64]LastOpInfo
	ownedShards        map[shardcfg.Tshid]bool // shard ID -> owned?
	shardVersions      map[shardcfg.Tshid]shardcfg.Tnum
	transferringShards map[shardcfg.Tshid]bool // shard is being transferred (freeze in effect)
}

type LastOpInfo struct {
	SeqNum int64
	Reply  CommandResult
}
type Op struct {
	Type     string
	Key      string
	Value    string
	Version  rpc.Tversion
	SeqNum   int64
	ClientId int64
	Shard    shardcfg.Tshid
	Num      shardcfg.Tnum
	State    []byte
}
type CommandResult struct {
	Err     rpc.Err
	Value   string
	Version rpc.Tversion
	State   []byte
	Num     shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	op := req.(Op)
	kv.mu.Lock()
	locked := true
	defer func() {
		if locked {
			kv.mu.Unlock()
		}
	}()
	kvLog(kv, "DoOp: %+v", op)
	if op.Type == "Put" {
		if shardHistory, ok := kv.lastApplied[op.Shard]; ok {
			if last, ok := shardHistory[op.ClientId]; ok {
				if op.SeqNum <= last.SeqNum {
					kvLog(kv, "DoOp: duplicate/stale request detected: %+v", op)
					return last.Reply
				}
			}
		}
	}

	var result CommandResult
	result.Err = rpc.OK

	// 2. 执行逻辑 (保持不变)
	switch op.Type {
	case "Get":
		shard := op.Shard
		if op.Key != "" {
			shard = shardcfg.Key2Shard(op.Key)
		}
		// if we don't own the shard, or it's currently transferring (frozen), reject
		if !kv.ownedShards[shard] || kv.transferringShards[shard] {
			kvLog(kv, "own:%v, trasns:%v", kv.ownedShards[shard], kv.transferringShards[shard])
			result.Err = rpc.ErrWrongGroup
			break
		}
		if shardData, ok := kv.data[shard]; ok {
			if v, ok := shardData[op.Key]; ok {
				result.Value = v.Value
				result.Err = rpc.OK
				result.Version = v.Version
			} else {
				result.Err = rpc.ErrNoKey
			}
		} else {
			result.Err = rpc.ErrNoKey
		}
		kvLog(kv, "myshards: %v", kv.ownedShards)
	case "Put":
		shard := op.Shard
		if op.Key != "" {
			shard = shardcfg.Key2Shard(op.Key)
		}
		// reject writes if we don't own the shard or if it's currently transferring
		if !kv.ownedShards[shard] || kv.transferringShards[shard] {
			result.Err = rpc.ErrWrongGroup
			break
		}
		var currentVersion rpc.Tversion = 0
		if shardData, ok := kv.data[shard]; ok {
			if v, ok := shardData[op.Key]; ok {
				currentVersion = v.Version
			}
		}

		if op.Version != currentVersion {
			result.Err = rpc.ErrVersion
			result.Version = currentVersion
			break // 拒绝写入
		}

		if kv.data[shard] == nil {
			kv.data[shard] = make(map[string]struct {
				Value   string
				Version rpc.Tversion
			})
		}
		kv.data[shard][op.Key] = struct {
			Value   string
			Version rpc.Tversion
		}{Value: op.Value, Version: currentVersion + 1}

		result.Err = rpc.OK
		result.Version = currentVersion + 1
	case "FreezeShard":
		shard := op.Shard
		num := op.Num

		currentVersion := kv.shardVersions[shard]
		skipFreeze := false
		switch {
		case num < currentVersion:
			result.Err = rpc.ErrWrongGroup
			skipFreeze = true
		case num == currentVersion:
			if !kv.ownedShards[shard] || kv.data[shard] == nil {
				result.Err = rpc.ErrWrongGroup
				skipFreeze = true
				break
			}
			kv.transferringShards[shard] = true
		case num > currentVersion:
			if !kv.ownedShards[shard] {
				result.Err = rpc.ErrWrongGroup
				skipFreeze = true
				break
			}
			kv.shardVersions[shard] = num
			// mark transferring (frozen) but don't clear data yet; Delete will clean up
			kv.transferringShards[shard] = true
		}
		if skipFreeze {
			break
		}

		// 复制当前 shard 数据，减小持锁时间
		dataMap := make(map[string]struct {
			Value   string
			Version rpc.Tversion
		})
		if shardData, ok := kv.data[shard]; ok {
			for k, v := range shardData {
				dataMap[k] = v
			}
		}
		clientMap := make(map[int64]LastOpInfo)
		if shardHistory, ok := kv.lastApplied[shard]; ok {
			for cid, lastOp := range shardHistory {
				clientMap[cid] = lastOp
			}
		}
		kv.shardVersions[shard] = num
		kvLog(kv, "FreezeShard: shard %d, num %d, current version %d myshards %v", shard, num, kv.shardVersions[shard], kv.ownedShards)
		locked = false
		kv.mu.Unlock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(dataMap)
		e.Encode(clientMap)
		encodedState := w.Bytes()
		kv.mu.Lock()
		locked = true
		result.State = encodedState
		result.Num = num
	case "InstallShard":
		shard := op.Shard
		currentVersion := kv.shardVersions[shard]
		if op.Num <= currentVersion {
			if kv.transferringShards[shard] {

				kvLog(kv, "Thaw shard %d via Install rollback: current version %d", shard, currentVersion)
			}
			result.Err = rpc.OK
			result.Num = currentVersion
			break
		}

		locked = false
		kv.mu.Unlock()
		r := bytes.NewBuffer(op.State)
		d := labgob.NewDecoder(r)
		var dataMap map[string]struct {
			Value   string
			Version rpc.Tversion
		}
		var clientMap map[int64]LastOpInfo
		if d.Decode(&dataMap) != nil ||
			d.Decode(&clientMap) != nil {
			panic("couldn't decode shard state")
		}
		kv.mu.Lock()
		locked = true
		kv.shardVersions[shard] = op.Num
		kv.ownedShards[shard] = true
		kv.transferringShards[shard] = false
		kv.data[shard] = make(map[string]struct {
			Value   string
			Version rpc.Tversion
		})
		for k, v := range dataMap {
			kv.data[shard][k] = v
		}
		if kv.lastApplied[shard] == nil {
			kv.lastApplied[shard] = make(map[int64]LastOpInfo)
		}
		for cid, v := range clientMap {
			if last, ok := kv.lastApplied[shard][cid]; !ok || v.SeqNum > last.SeqNum {
				kv.lastApplied[shard][cid] = v
			}
		}
		kvLog(kv, "InstallShard: shard %d, num %d, current version %d,myshards %v transferring %v", shard, op.Num, kv.shardVersions[shard], kv.ownedShards, kv.transferringShards[shard])
		result.Num = kv.shardVersions[shard]
		result.Err = rpc.OK
	case "DeleteShard":
		shard := op.Shard
		num := op.Num
		if op.Num < kv.shardVersions[shard] {
			result.Err = rpc.OK
			break
		}
		// finalize transfer: advance version, clear ownership and data
		kv.shardVersions[shard] = num
		kv.ownedShards[shard] = false
		kv.transferringShards[shard] = false
		kv.data[shard] = nil
		delete(kv.lastApplied, shard)
		kvLog(kv, "DeleteShard: shard %d, num %d, current version %d,myshards %v transferring %v", shard, num, kv.shardVersions[shard], kv.ownedShards, kv.transferringShards[shard])
		result.Num = num
		result.Err = rpc.OK

	default:
		// unknown command
	}
	if op.Type == "Put" && result.Err != rpc.ErrWrongGroup {
		if kv.lastApplied[op.Shard] == nil {
			kv.lastApplied[op.Shard] = make(map[int64]LastOpInfo)
		}
		kv.lastApplied[op.Shard][op.ClientId] = LastOpInfo{SeqNum: op.SeqNum, Reply: result}
	}

	kvLog(kv, "DoOp: result: %+v", result)
	return result
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e.Encode(kv.data)
	e.Encode(kv.lastApplied)
	e.Encode(kv.shardVersions)
	e.Encode(kv.ownedShards)
	e.Encode(kv.transferringShards)
	return w.Bytes()

}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storedData map[shardcfg.Tshid]map[string]struct {
		Value   string
		Version rpc.Tversion
	}
	var lastApplied map[shardcfg.Tshid]map[int64]LastOpInfo
	var shardVersions map[shardcfg.Tshid]shardcfg.Tnum
	var ownedShards map[shardcfg.Tshid]bool
	var transferring map[shardcfg.Tshid]bool
	if d.Decode(&storedData) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&shardVersions) != nil ||
		d.Decode(&ownedShards) != nil ||
		d.Decode(&transferring) != nil {
		panic("couldn't decode data")
	}
	kvLog(kv, "Restore:  lastApplied: %+v, shardVersions: %+v, ownedShards: %+v transferring: %+v", lastApplied, shardVersions, ownedShards, transferring)
	kv.mu.Lock()
	kv.data = storedData
	kv.lastApplied = lastApplied

	kv.shardVersions = shardVersions
	kv.ownedShards = ownedShards
	kv.transferringShards = transferring
	if kv.data == nil {
		kv.data = make(map[shardcfg.Tshid]map[string]struct {
			Value   string
			Version rpc.Tversion
		})
	}
	if kv.lastApplied == nil {
		kv.lastApplied = make(map[shardcfg.Tshid]map[int64]LastOpInfo)
	}
	if kv.ownedShards == nil {
		kv.ownedShards = make(map[shardcfg.Tshid]bool)
	}
	if kv.shardVersions == nil {
		kv.shardVersions = make(map[shardcfg.Tshid]shardcfg.Tnum)
	}
	if kv.transferringShards == nil {
		kv.transferringShards = make(map[shardcfg.Tshid]bool)
	}
	for i := 0; i < shardcfg.NShards; i++ {
		shard := shardcfg.Tshid(i)
		if _, ok := kv.data[shard]; !ok {
			kv.data[shard] = make(map[string]struct {
				Value   string
				Version rpc.Tversion
			})
		}
		if _, ok := kv.ownedShards[shard]; !ok {
			kv.ownedShards[shard] = false
		}
		if _, ok := kv.shardVersions[shard]; !ok {
			kv.shardVersions[shard] = 0
		}
		if _, ok := kv.transferringShards[shard]; !ok {
			kv.transferringShards[shard] = false
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	shard := shardcfg.Key2Shard(args.Key)
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
		Shard:    shard,
	}
	err, val := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	result := val.(CommandResult)
	reply.Err = result.Err
	reply.Value = result.Value
	reply.Version = result.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	shard := shardcfg.Key2Shard(args.Key)
	op := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		Version:  args.Version,
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
		Shard:    shard,
	}
	err, val := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	result := val.(CommandResult)
	reply.Err = result.Err
	reply.Version = result.Version
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	op := Op{
		Type:  "FreezeShard",
		Shard: args.Shard,
		Num:   args.Num,
	}
	kvLog(kv, "start Freeze")
	err, val := kv.rsm.Submit(op)
	if err != rpc.OK {
		kvLog(kv, "FreezeShard failed")
		reply.Err = err
		return
	}
	result := val.(CommandResult)
	reply.Err = result.Err

	reply.State = result.State
	reply.Num = result.Num
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	op := Op{
		Type:  "InstallShard",
		Shard: args.Shard,
		State: args.State,
		Num:   args.Num,
	}
	err, val := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	result := val.(CommandResult)
	reply.Err = result.Err

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	op := Op{
		Type:  "DeleteShard",
		Shard: args.Shard,
		Num:   args.Num,
	}
	kvLog(kv, "start Delete")
	err, val_ := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		kvLog(kv, "d fail %v", err)
		return
	}
	result := val_.(CommandResult)
	kvLog(kv, "Delete complete %v", result)
	reply.Err = result.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.data = make(map[shardcfg.Tshid]map[string]struct {
		Value   string
		Version rpc.Tversion
	})
	kv.lastApplied = make(map[shardcfg.Tshid]map[int64]LastOpInfo)

	// Your code here
	kv.ownedShards = make(map[shardcfg.Tshid]bool)
	kv.transferringShards = make(map[shardcfg.Tshid]bool)
	for i := 0; i < shardcfg.NShards; i++ {
		shard := shardcfg.Tshid(i)
		kv.data[shard] = make(map[string]struct {
			Value   string
			Version rpc.Tversion
		})
		owned := false
		if kv.gid == shardcfg.Gid1 {
			owned = true
		}
		kv.ownedShards[shard] = owned
		kv.transferringShards[shard] = false
	}
	kv.shardVersions = make(map[shardcfg.Tshid]shardcfg.Tnum)
	for i := 0; i < shardcfg.NShards; i++ {
		kv.shardVersions[shardcfg.Tshid(i)] = 0
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
