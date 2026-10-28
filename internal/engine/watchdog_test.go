package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/easytier/easytier/easytier-go/platform/netstd"
)

func TestZeroDeadlineGuard(t *testing.T) {
	var guard zeroDeadlineGuard
	for range zeroDeadlineGuardBudget {
		if got := guard.deadline(0); got != 0 {
			t.Fatalf("deadline within budget = %d, want 0", got)
		}
		guard.timerDrive(0)
	}
	if got := guard.deadline(0); got != 1 {
		t.Fatalf("deadline after %d immediate timer drives = %d, want 1", zeroDeadlineGuardBudget, got)
	}
	if got := guard.deadline(25); got != 25 {
		t.Fatalf("a real deadline is kept: got %d", got)
	}
	guard.reset()
	if got := guard.deadline(0); got != 0 {
		t.Fatalf("deadline after another wake-up = %d, want 0", got)
	}
	guard.timerDrive(0)
	guard.timerDrive(5)
	if guard.consecutive != 0 {
		t.Fatal("a non-zero deadline resets the guard")
	}
}

func TestGuestLockReportsBusyTime(t *testing.T) {
	var lock guestLock
	if busy := lock.busyFor(time.Since(watchdogEpoch)); busy != 0 {
		t.Fatalf("idle lock busy for %s", busy)
	}
	lock.Lock()
	time.Sleep(20 * time.Millisecond)
	if busy := lock.busyFor(time.Since(watchdogEpoch)); busy < 20*time.Millisecond {
		t.Fatalf("held lock busy for %s", busy)
	}
	lock.Unlock()
	if busy := lock.busyFor(time.Since(watchdogEpoch)); busy != 0 {
		t.Fatalf("released lock busy for %s", busy)
	}
}

func TestWatchdogClosesStuckGuest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := NewHost(ctx, Options{Services: netstd.Services(), GuestCallTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close(ctx)
	instance, err := host.CreateInstance(ctx, minimalConfig)
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	// Hold the guest as a call that never returns would. The timeout leaves
	// room for slow legitimate calls under the race detector.
	instance.guestMu.Lock()
	time.Sleep(5 * time.Second)
	instance.guestMu.Unlock()
	select {
	case <-instance.done:
	case <-time.After(10 * time.Second):
		t.Fatal("instance with a stuck guest did not end")
	}
	if err := instance.TerminalError(); err == nil || !strings.Contains(err.Error(), "did not return within") {
		t.Fatalf("terminal error = %v, want the watchdog cause", err)
	}
	// The host and its other guests keep working.
	other, err := host.CreateInstance(ctx, minimalConfig)
	if err != nil {
		t.Fatalf("create instance after watchdog: %v", err)
	}
	if err := other.Close(ctx); err != nil {
		t.Fatalf("close instance after watchdog: %v", err)
	}
}
