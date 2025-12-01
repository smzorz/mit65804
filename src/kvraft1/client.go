package kvraft

import (
	crand "crypto/rand"
	"log"
	"math/big"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

const ckDebug = false

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
func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leaderId = 0
	ck.seqNum = 0
	ck.clientId = nrand()
	return ck
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (ck *Clerk) advanceLeader() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	backoff := 50*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond
	time.Sleep(backoff)
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	ck.seqNum++
	for {

		args := &rpc.GetArgs{Key: key, SeqNum: ck.seqNum, ClientId: ck.clientId}
		ckLog("Client: %d Get: %+v", ck.clientId, args)
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[ck.leaderId], "KVServer.Get", args, &reply)
		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				ckLog("Client: %d Get: reply: %+v", ck.clientId, reply)
				return reply.Value, reply.Version, reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				ck.advanceLeader()
				continue
			}
		}
		ck.advanceLeader()
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	ck.seqNum++
	args := &rpc.PutArgs{Key: key, Value: value, Version: version, SeqNum: ck.seqNum, ClientId: ck.clientId}
	var retried bool
	retried = false
	for {
		ckLog("Put: %+v", args)
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[ck.leaderId], "KVServer.Put", args, &reply)
		if ok {
			ckLog("Put: reply: %+v", reply)
			switch reply.Err {
			case rpc.OK:
				ckLog("Put: reply: %+v", reply)
				return rpc.OK
			case rpc.ErrVersion:
				if retried {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			case rpc.ErrWrongLeader:
				// rotate below
			case rpc.ErrNoKey:
				if retried {
					return rpc.ErrMaybe
				}
				return rpc.ErrNoKey
			default:
				if retried {
					return rpc.ErrMaybe
				}
				return reply.Err
			}
		} else {
			retried = true
		}
		ck.advanceLeader()
	}
}
