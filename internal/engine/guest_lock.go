package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

// watchdogEpoch anchors the monotonic busy timestamps of guest locks.
var watchdogEpoch = time.Now()

// guestLock serializes the calls into one guest and records since when the
// current call has been running, which the host's watchdog reads without
// taking the lock.
type guestLock struct {
	mu sync.Mutex
	// busySince is the monotonic time the lock was taken, or zero.
	busySince atomic.Int64
}

func (lock *guestLock) Lock() {
	lock.mu.Lock()
	// The low bit keeps a call starting at the epoch distinguishable from
	// an idle guest.
	lock.busySince.Store(int64(time.Since(watchdogEpoch)) | 1)
}

func (lock *guestLock) Unlock() {
	lock.busySince.Store(0)
	lock.mu.Unlock()
}

// busyFor reports how long the current call has been running at now, a
// duration since watchdogEpoch, or zero when the guest is idle.
func (lock *guestLock) busyFor(now time.Duration) time.Duration {
	since := lock.busySince.Load()
	if since == 0 {
		return 0
	}
	return now - time.Duration(since)
}

// zeroDeadlineGuardBudget is how many consecutive timer drives may ask to be
// driven again at once before the driver waits a millisecond.
const zeroDeadlineGuardBudget = 16

// zeroDeadlineGuard bounds busy driving on an idle guest. A guest reports its
// next deadline in whole milliseconds; one that rounds a sub-millisecond
// timer down to zero would otherwise be driven in a tight loop until the
// timer expires. Any other wake-up resets the guard, so a guest with real
// work is never delayed.
type zeroDeadlineGuard struct {
	consecutive int
}

func (guard *zeroDeadlineGuard) deadline(deadline int64) int64 {
	if deadline == 0 && guard.consecutive >= zeroDeadlineGuardBudget {
		return 1
	}
	return deadline
}

func (guard *zeroDeadlineGuard) timerDrive(next int64) {
	if next == 0 {
		guard.consecutive++
	} else {
		guard.consecutive = 0
	}
}

func (guard *zeroDeadlineGuard) reset() {
	guard.consecutive = 0
}
