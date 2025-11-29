package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	db map[string]struct {
		value   string
		version rpc.Tversion
	}
	mu          sync.Mutex
	lastApplied map[int64]LastOpInfo // map from ClientId to last applied SeqNum
}
type LastOpInfo struct {
	LastSeqNum int64
	Reply      CommandResult
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
	if lastInfo, ok := kv.lastApplied[op.ClientId]; ok {

		if op.SeqNum <= lastInfo.LastSeqNum {
			// Duplicate request
			return lastInfo.Reply
		}
	}
	var result CommandResult
	result.Err = rpc.OK
	switch op.Type {
	case "Get":
		if v, ok := kv.db[op.Key]; ok {
			result.Value = v.value
			result.Err = rpc.OK
			result.Version = v.version
		} else {
			result.Err = rpc.ErrNoKey
		}
	case "Put":
		var oldVersion rpc.Tversion = 0
		if v, ok := kv.db[op.Key]; ok {
			oldVersion = v.version
			if oldVersion != op.Version {
				result.Err = rpc.ErrVersion
				result.Version = oldVersion
				break
			}
		}
		kv.db[op.Key] = struct {
			value   string
			version rpc.Tversion
		}{value: op.Value, version: oldVersion + 1}

	default:
		// unknown command
	}
	kv.lastApplied[op.ClientId] = LastOpInfo{
		LastSeqNum: op.SeqNum,
		Reply:      result,
	}

	return result

	// Check for duplicate requests

}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
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
		value   string
		version rpc.Tversion
	})
	kv.mu = sync.Mutex{}
	kv.lastApplied = make(map[int64]LastOpInfo)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
