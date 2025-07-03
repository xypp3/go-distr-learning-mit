package lock

import (
	"sync"
	"time"

	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	mu   sync.Mutex
	lock map[string]bool
	id   string
	l    string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lock = make(map[string]bool)
	lk.lock[l] = false
	lk.id = kvtest.RandValue(8)
	lk.l = l

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		lk.mu.Lock()
		v, _ := lk.lock[lk.l]
		if !v {
			v = true
			break
		}
		lk.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	lk.mu.Unlock()
}

func (lk *Lock) Release() {
	// Your code here
	lk.mu.Lock()
	defer lk.mu.Unlock()
	lk.lock[lk.l] = false
}
