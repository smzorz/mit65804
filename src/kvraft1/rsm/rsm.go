package rsm

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me      int
	Command any
	OpId    int64
}
type OpResult struct {
	Value any
	OpId  int64
	Term  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	notifyChans map[int]chan OpResult // map from log index to notify channel
	deadCh      chan struct{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		notifyChans:  make(map[int]chan OpResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	rsm.deadCh = make(chan struct{})
	go rsm.Reader()
	return rsm
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
			result := rsm.sm.DoOp(op.Command)
			rsm.mu.Lock()
			if ch, ok := rsm.notifyChans[msg.CommandIndex]; ok {
				ch <- OpResult{Value: result, OpId: op.OpId, Term: 0}
			}
			rsm.mu.Unlock()
		}
	}

	rsm.mu.Lock()
	close(rsm.deadCh)
	for _, ch := range rsm.notifyChans {
		close(ch)
	}
	rsm.mu.Unlock()

}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	myId := nrand()
	op := Op{Me: rsm.me, Command: req, OpId: myId}

	index, startTerm, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	ch := make(chan OpResult, 1)
	rsm.mu.Lock()
	rsm.notifyChans[index] = ch
	rsm.mu.Unlock()
	defer func() {
		rsm.mu.Lock()
		delete(rsm.notifyChans, index)
		rsm.mu.Unlock()
	}()
	// wait for command to be applied or timeout
	// your code here

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case result, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			if result.OpId != myId {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, result.Value

		case <-ticker.C:
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || currentTerm != startTerm {
				return rpc.ErrWrongLeader, nil // 失败，退出
			}

		case <-rsm.deadCh:
			return rpc.ErrWrongLeader, nil // 关机，退出
		}
	}

	// i'm dead, try another server.

}
