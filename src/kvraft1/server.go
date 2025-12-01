package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const kvDebug = false

func kvLog(kv *KVServer, format string, a ...interface{}) {
	if !kvDebug {
		return
	}
	log.Printf("[S%d] "+format, append([]interface{}{kv.me}, a...)...)
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	db map[string]struct {
		Value   string
		Version rpc.Tversion
	}
	mu          sync.Mutex
	lastApplied map[int64]LastOpInfo
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
}
type CommandResult struct {
	Err     rpc.Err
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	op := req.(Op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kvLog(kv, "DoOp: %+v", op)

	if last, ok := kv.lastApplied[op.ClientId]; ok {
		if op.SeqNum <= last.SeqNum {
			kvLog(kv, "DoOp: duplicate/stale request detected: %+v", op)
			return last.Reply
		}
	}

	var result CommandResult
	result.Err = rpc.OK

	// 2. 执行逻辑 (保持不变)
	switch op.Type {
	case "Get":
		if v, ok := kv.db[op.Key]; ok {
			result.Value = v.Value
			result.Err = rpc.OK
			result.Version = v.Version
		} else {
			result.Err = rpc.ErrNoKey
		}
	case "Put":
		var currentVersion rpc.Tversion = 0
		if v, ok := kv.db[op.Key]; ok {
			currentVersion = v.Version
		}

		if op.Version != currentVersion {
			result.Err = rpc.ErrVersion
			result.Version = currentVersion
			break // 拒绝写入
		}

		kv.db[op.Key] = struct {
			Value   string
			Version rpc.Tversion
		}{Value: op.Value, Version: currentVersion + 1}

		result.Err = rpc.OK
		result.Version = currentVersion + 1
	default:
		// unknown command
	}

	kv.lastApplied[op.ClientId] = LastOpInfo{SeqNum: op.SeqNum, Reply: result}

	kvLog(kv, "DoOp: result: %+v", result)
	return result
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e.Encode(kv.db)
	e.Encode(kv.lastApplied)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]struct {
		Value   string
		Version rpc.Tversion
	}
	var lastApplied map[int64]LastOpInfo
	if d.Decode(&db) != nil ||
		d.Decode(&lastApplied) != nil {
		panic("couldn't decode data")
	}
	kv.mu.Lock()
	kv.db = db
	kv.lastApplied = lastApplied
	if kv.lastApplied == nil {
		kv.lastApplied = make(map[int64]LastOpInfo)
	}
	kv.mu.Unlock()

}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	op := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		Version:  args.Version,
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(Op{})
	kv := &KVServer{me: me}
	kv.db = make(map[string]struct {
		Value   string
		Version rpc.Tversion
	})
	kv.mu = sync.Mutex{}
	kv.lastApplied = make(map[int64]LastOpInfo)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
