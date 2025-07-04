package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lock_key string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = kvtest.RandValue(8)
	lk.lock_key = l

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	id, ver, err := lk.ck.Get(lk.lock_key)
	if err == rpc.ErrNoKey {
		lk.ck.Put(lk.lock_key, "", 0)
	}

	found_maybe := false
	for {
		if found_maybe && id == lk.id {
			break
		} else if err == rpc.OK && id == "" {
			err_p := lk.ck.Put(lk.lock_key, lk.id, ver)
			if err_p == rpc.OK {
				break
			}
			if err_p == rpc.ErrMaybe {
				found_maybe = true
			}
		}
		time.Sleep(10 * time.Millisecond)
		id, ver, err = lk.ck.Get(lk.lock_key)
	}
}

func (lk *Lock) Release() {
	// Your code here
	id, ver, err := lk.ck.Get(lk.lock_key)
	for i := 0; true; i++ {
		if id == "" {
			break
		} else if err == rpc.OK && id == lk.id {
			err_p := lk.ck.Put(lk.lock_key, "", ver)

			if err_p == rpc.OK {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		id, ver, err = lk.ck.Get(lk.lock_key)
	}
}
