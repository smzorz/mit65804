package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lock_name string       // key name to place in k/v map
	id        string       // client id
	version   rpc.Tversion // version number
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.ck = ck
	lk.lock_name = l
	lk.id = kvtest.RandValue(8)
	lk.version = 0
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	for {
		value, version, err := lk.ck.Get(lk.lock_name)
		if err == rpc.ErrNoKey {
			put_err := lk.ck.Put(lk.lock_name, lk.id, 0)
			switch put_err {
			case rpc.OK:
				lk.version = rpc.Tversion(1)
				return
			case rpc.ErrMaybe:
				value, _, err := lk.ck.Get(lk.lock_name)
				if err == rpc.OK && value == lk.id {
					lk.version = rpc.Tversion(1)
					return
				}
			}
		} else if err == rpc.OK && value == "" {
			put_err := lk.ck.Put(lk.lock_name, lk.id, version)
			switch put_err {
			case rpc.OK:
				lk.version = rpc.Tversion(version + 1)
				return
			case rpc.ErrMaybe:
				value, _, err := lk.ck.Get(lk.lock_name)
				if err == rpc.OK && value == lk.id {
					lk.version = rpc.Tversion(version + 1)
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.lock_name)
		if err == rpc.OK && value == lk.id {
			put_err := lk.ck.Put(lk.lock_name, "", version)
			switch put_err {
			case rpc.OK:
				lk.version = 0
				return
			case rpc.ErrMaybe:
				value, _, err := lk.ck.Get(lk.lock_name)
				if err == rpc.OK && value == "" {
					lk.version = 0
					return
				}
			}
		} else if err == rpc.ErrNoKey {
			lk.version = 0
			return
		} else {
			// either err != OK or value != lk.id
			// means the lock is not held by this client
			panic("lock not held by client")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
