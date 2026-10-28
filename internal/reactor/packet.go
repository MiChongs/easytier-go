package reactor

import (
	"context"
	"fmt"
	"net"
)

// packetSink queues the IP packets a guest writes for the embedder. Packets
// are copied into pooled buffers and kept in a ring, and the guest operations
// waiting for room are indexed by the sink they wait on.
type packetSink struct {
	capacity  int
	ring      [][]byte
	head      int
	count     int
	waiters   map[uint64]*packetWriteWaiter
	available chan struct{}
	closed    chan struct{}
}

type packetWriteWaiter struct {
	handle uint64
	ready  bool
}

func (sink *packetSink) push(packet []byte) {
	sink.ring[(sink.head+sink.count)%len(sink.ring)] = packet
	sink.count++
}

func (sink *packetSink) pop() []byte {
	packet := sink.ring[sink.head]
	sink.ring[sink.head] = nil
	sink.head = (sink.head + 1) % len(sink.ring)
	sink.count--
	return packet
}

// wakeWritersLocked marks the guest operations waiting for room ready and
// reports whether there were any. A waiter already marked ready is signaled
// again: the guest may have polled it before the change it waits for.
func (sink *packetSink) wakeWritersLocked() bool {
	for _, waiter := range sink.waiters {
		waiter.ready = true
	}
	return len(sink.waiters) > 0
}

func (reactor *Reactor) RegisterPacketSink(capacity int) (uint64, error) {
	if capacity <= 0 {
		return 0, fmt.Errorf("packet sink capacity must be positive")
	}
	reactor.mu.Lock()
	defer reactor.mu.Unlock()
	if reactor.closed {
		return 0, ErrInvalid
	}
	handle := reactor.allocateHandleLocked()
	reactor.packetSinks[handle] = &packetSink{
		capacity:  capacity,
		ring:      make([][]byte, capacity),
		waiters:   make(map[uint64]*packetWriteWaiter),
		available: make(chan struct{}, 1),
		closed:    make(chan struct{}),
	}
	return handle, nil
}

func (reactor *Reactor) UnregisterPacketSink(handle uint64) {
	reactor.mu.Lock()
	sink := reactor.packetSinks[handle]
	if sink == nil {
		reactor.mu.Unlock()
		return
	}
	delete(reactor.packetSinks, handle)
	for operation := range sink.waiters {
		delete(reactor.packetWrites, operation)
		reactor.releaseOperationLocked(operation, operationPacketWrite)
	}
	for sink.count > 0 {
		putBuffer(sink.pop())
	}
	close(sink.closed)
	reactor.mu.Unlock()
}

func (reactor *Reactor) TryPacketWrite(handle uint64, packet []byte) error {
	reactor.mu.Lock()
	sink, exists := reactor.packetSinks[handle]
	if !exists {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if sink.count >= sink.capacity {
		reactor.mu.Unlock()
		return ErrWouldBlock
	}
	sink.push(append(getBuffer(len(packet))[:0], packet...))
	available := sink.available
	reactor.mu.Unlock()
	select {
	case available <- struct{}{}:
	default:
	}
	return nil
}

func (reactor *Reactor) StartPacketWriteReady(handle, operation uint64) error {
	reactor.mu.Lock()
	sink, exists := reactor.packetSinks[handle]
	if !exists {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if err := reactor.claimOperationLocked(operation, operationPacketWrite); err != nil {
		reactor.mu.Unlock()
		return err
	}
	waiter := &packetWriteWaiter{
		handle: handle,
		ready:  sink.count < sink.capacity,
	}
	reactor.packetWrites[operation] = waiter
	sink.waiters[operation] = waiter
	ready := waiter.ready
	reactor.mu.Unlock()
	if ready {
		reactor.signalCompletion()
	}
	return nil
}

func (reactor *Reactor) TakePacketWriteReady(operation uint64) error {
	reactor.mu.Lock()
	waiter, exists := reactor.packetWrites[operation]
	if !exists || reactor.operations[operation] != operationPacketWrite {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if !waiter.ready {
		reactor.mu.Unlock()
		return ErrPending
	}
	reactor.forgetPacketWriteLocked(operation, waiter)
	reactor.releaseOperationLocked(operation, operationPacketWrite)
	reactor.mu.Unlock()
	return nil
}

func (reactor *Reactor) forgetPacketWriteLocked(operation uint64, waiter *packetWriteWaiter) {
	delete(reactor.packetWrites, operation)
	if waiter == nil {
		return
	}
	if sink := reactor.packetSinks[waiter.handle]; sink != nil {
		delete(sink.waiters, operation)
	}
}

// ReceivePacket waits for one packet. The packet belongs to the caller, which
// may hand it back with ReleasePacket.
func (reactor *Reactor) ReceivePacket(ctx context.Context, handle uint64) ([]byte, error) {
	var packets [1][]byte
	if _, err := reactor.ReceivePackets(ctx, handle, packets[:]); err != nil {
		return nil, err
	}
	return packets[0], nil
}

// ReceivePackets waits for at least one packet and fills packets with as many
// queued ones as fit, returning how many it stored. The packets belong to the
// caller, which may hand them back with ReleasePacket.
func (reactor *Reactor) ReceivePackets(ctx context.Context, handle uint64, packets [][]byte) (int, error) {
	if len(packets) == 0 {
		return 0, nil
	}
	for {
		reactor.mu.Lock()
		sink, exists := reactor.packetSinks[handle]
		if !exists {
			reactor.mu.Unlock()
			return 0, fmt.Errorf("receive packet: %w", net.ErrClosed)
		}
		if sink.count > 0 {
			n := 0
			for n < len(packets) && sink.count > 0 {
				packets[n] = sink.pop()
				n++
			}
			ready := sink.wakeWritersLocked()
			reactor.mu.Unlock()
			if ready {
				reactor.signalCompletion()
			}
			return n, nil
		}
		available := sink.available
		lifetimeDone := reactor.ctx.Done()
		reactor.mu.Unlock()

		select {
		case <-available:
		case <-sink.closed:
			return 0, fmt.Errorf("receive packet: %w", net.ErrClosed)
		case <-lifetimeDone:
			return 0, fmt.Errorf("receive packet: %w", reactor.ctx.Err())
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}
