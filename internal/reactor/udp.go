package reactor

import (
	"fmt"
	"net"
	"net/netip"
	"sync"
)

const (
	udpReceiveQueueCapacity = 64
	udpSendQueueCapacity    = 64
	udpMaxDatagram          = 65535
)

// Datagram is a received UDP payload with its sender.
type Datagram struct {
	Data []byte
	Peer *net.UDPAddr
}

type udpDatagram struct {
	data []byte
	peer netip.AddrPort
}

type udpSend struct {
	data []byte
	peer netip.AddrPort
}

// Sockets that can exchange netip.AddrPort, such as *net.UDPConn, are used
// without allocating a net.Addr per datagram.
type addrPortReader interface {
	ReadFromUDPAddrPort([]byte) (int, netip.AddrPort, error)
}

type addrPortWriter interface {
	WriteToUDPAddrPort([]byte, netip.AddrPort) (int, error)
}

type datagramState struct {
	handle         uint64
	connection     net.PacketConn
	receiveBuffer  []byte
	received       ring[udpDatagram]
	receiveRunning bool
	receiveErr     error
	// readWaiters and writeWaiters are the guest operations waiting on this
	// socket, so a datagram wakes them without scanning every operation.
	readWaiters  map[uint64]*udpReadWaiter
	writeWaiters map[uint64]*udpWriteWaiter
	sendQueue    chan udpSend
	sendErr      error
	closeOnce    sync.Once
}

type udpReadWaiter struct {
	state *datagramState
	ready bool
}

type udpWriteWaiter struct {
	state *datagramState
	ready bool
}

func newDatagramState(handle uint64, connection net.PacketConn) *datagramState {
	return &datagramState{
		handle:       handle,
		connection:   connection,
		received:     newRing[udpDatagram](udpReceiveQueueCapacity),
		readWaiters:  make(map[uint64]*udpReadWaiter),
		writeWaiters: make(map[uint64]*udpWriteWaiter),
		sendQueue:    make(chan udpSend, udpSendQueueCapacity),
	}
}

func (state *datagramState) closeAfterQueuedSends() {
	state.closeOnce.Do(func() {
		close(state.sendQueue)
	})
}

func (state *datagramState) closeNow() {
	state.closeOnce.Do(func() {
		close(state.sendQueue)
	})
	_ = state.connection.Close()
}

func (state *datagramState) wakeReadersLocked() bool {
	for _, waiter := range state.readWaiters {
		waiter.ready = true
	}
	return len(state.readWaiters) > 0
}

func (state *datagramState) wakeWritersLocked() bool {
	for _, waiter := range state.writeWaiters {
		waiter.ready = true
	}
	return len(state.writeWaiters) > 0
}

func (state *datagramState) receive(buffer []byte) (int, netip.AddrPort, error) {
	if reader, ok := state.connection.(addrPortReader); ok {
		return reader.ReadFromUDPAddrPort(buffer)
	}
	n, peer, err := state.connection.ReadFrom(buffer)
	if err != nil {
		return n, netip.AddrPort{}, err
	}
	udpPeer, ok := peer.(*net.UDPAddr)
	if !ok {
		return n, netip.AddrPort{}, fmt.Errorf("unsupported UDP peer address %T", peer)
	}
	return n, udpAddrPort(udpPeer), nil
}

func (state *datagramState) send(data []byte, peer netip.AddrPort) (int, error) {
	if writer, ok := state.connection.(addrPortWriter); ok {
		return writer.WriteToUDPAddrPort(data, peer)
	}
	return state.connection.WriteTo(data, net.UDPAddrFromAddrPort(peer))
}

func (reactor *Reactor) runUDPSends(handle uint64, state *datagramState) {
	defer reactor.workers.Done()
	defer reactor.finishUDPSends(state)
	defer state.connection.Close()

	for request := range state.sendQueue {
		reactor.mu.Lock()
		ready := state.wakeWritersLocked()
		reactor.mu.Unlock()
		if ready {
			reactor.signalCompletion()
		}

		n, err := state.send(request.data, request.peer)
		if err == nil && n != len(request.data) {
			err = fmt.Errorf("short UDP write: %d of %d", n, len(request.data))
		}
		putBuffer(request.data)
		if err != nil {
			reactor.mu.Lock()
			ready := false
			if reactor.datagrams[handle] == state {
				state.sendErr = err
				ready = state.wakeWritersLocked()
			}
			reactor.mu.Unlock()
			if ready {
				reactor.signalCompletion()
			}
		}
	}
}

func (reactor *Reactor) finishUDPSends(state *datagramState) {
	reactor.mu.Lock()
	delete(reactor.drainingDatagrams, state)
	reactor.mu.Unlock()
}

func (reactor *Reactor) StartUDPReceive(handle, operation uint64) error {
	reactor.mu.Lock()
	state, exists := reactor.datagrams[handle]
	if !exists {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if err := reactor.claimOperationLocked(operation, operationUDPRead); err != nil {
		reactor.mu.Unlock()
		return err
	}
	waiter := &udpReadWaiter{
		state: state,
		ready: state.received.len() > 0 || state.receiveErr != nil,
	}
	reactor.udpReads[operation] = waiter
	state.readWaiters[operation] = waiter
	startWorker := reactor.startUDPReceiveLocked(state)
	reactor.mu.Unlock()

	if startWorker {
		go reactor.runUDPReceive(handle, state)
	}
	if waiter.ready {
		reactor.signalCompletion()
	}
	return nil
}

// startUDPReceiveLocked claims the receive worker of state when the socket
// should be read and no worker runs.
func (reactor *Reactor) startUDPReceiveLocked(state *datagramState) bool {
	if state.receiveErr != nil || state.received.full() || state.receiveRunning {
		return false
	}
	state.receiveRunning = true
	reactor.workers.Add(1)
	return true
}

// runUDPReceive reads datagrams until the queue is full or the socket fails.
// It keeps reading between guest polls, so a burst does not start a worker
// per datagram; the bounded queue and the kernel buffer provide backpressure.
func (reactor *Reactor) runUDPReceive(handle uint64, state *datagramState) {
	defer reactor.workers.Done()
	if state.receiveBuffer == nil {
		state.receiveBuffer = make([]byte, udpMaxDatagram)
	}
	buffer := state.receiveBuffer
	for {
		n, peer, err := state.receive(buffer)

		reactor.mu.Lock()
		if reactor.datagrams[handle] != state {
			reactor.mu.Unlock()
			return
		}
		if err != nil {
			state.receiveErr = err
		} else {
			state.received.push(udpDatagram{
				data: append(getBuffer(n)[:0], buffer[:n]...),
				peer: peer,
			})
		}
		ready := state.wakeReadersLocked()
		continueReceiving := err == nil && !state.received.full()
		if !continueReceiving {
			state.receiveRunning = false
		}
		reactor.mu.Unlock()
		if ready {
			reactor.signalCompletion()
		}
		if !continueReceiving {
			return
		}
	}
}

// TakeUDPReceive completes a receive and returns a copy of the datagram.
func (reactor *Reactor) TakeUDPReceive(operation uint64, capacity uint32) (Datagram, error) {
	var datagram Datagram
	_, err := reactor.TakeUDPReceiveInto(operation, capacity, func(data []byte, peer netip.AddrPort) error {
		datagram = Datagram{
			Data: append([]byte(nil), data...),
			Peer: net.UDPAddrFromAddrPort(peer),
		}
		return nil
	})
	return datagram, err
}

// TakeUDPReceiveInto completes a receive by passing the datagram, truncated
// to capacity, to deliver, which must copy it out before returning. It
// returns the number of bytes delivered.
func (reactor *Reactor) TakeUDPReceiveInto(
	operation uint64,
	capacity uint32,
	deliver func(data []byte, peer netip.AddrPort) error,
) (int, error) {
	reactor.mu.Lock()
	waiter, exists := reactor.udpReads[operation]
	if !exists || reactor.operations[operation] != operationUDPRead {
		reactor.mu.Unlock()
		return 0, ErrInvalid
	}
	state := waiter.state
	handle, exists := reactor.datagramHandleLocked(state)
	if !exists {
		reactor.forgetUDPReadLocked(operation, waiter)
		reactor.releaseOperationLocked(operation, operationUDPRead)
		reactor.mu.Unlock()
		return 0, ErrInvalid
	}
	if state.received.len() == 0 {
		if state.receiveErr == nil {
			reactor.mu.Unlock()
			return 0, ErrPending
		}
		err := state.receiveErr
		state.receiveErr = nil
		reactor.forgetUDPReadLocked(operation, waiter)
		reactor.releaseOperationLocked(operation, operationUDPRead)
		startWorker := len(state.readWaiters) > 0 && reactor.startUDPReceiveLocked(state)
		reactor.mu.Unlock()
		if startWorker {
			go reactor.runUDPReceive(handle, state)
		}
		return 0, err
	}

	datagram := state.received.pop()
	data := datagram.data
	if uint32(len(data)) > capacity {
		data = data[:capacity]
	}
	deliverErr := deliver(data, datagram.peer)
	putBuffer(datagram.data)
	reactor.forgetUDPReadLocked(operation, waiter)
	reactor.releaseOperationLocked(operation, operationUDPRead)
	startWorker := reactor.startUDPReceiveLocked(state)
	reactor.mu.Unlock()

	if startWorker {
		go reactor.runUDPReceive(handle, state)
	}
	return len(data), deliverErr
}

// datagramHandleLocked reports the handle of state while it is registered. A
// closed socket is no longer registered.
func (reactor *Reactor) datagramHandleLocked(state *datagramState) (uint64, bool) {
	return state.handle, reactor.datagrams[state.handle] == state
}

func (reactor *Reactor) forgetUDPReadLocked(operation uint64, waiter *udpReadWaiter) {
	delete(reactor.udpReads, operation)
	if waiter != nil {
		delete(waiter.state.readWaiters, operation)
	}
}

func (reactor *Reactor) forgetUDPWriteLocked(operation uint64, waiter *udpWriteWaiter) {
	delete(reactor.udpWrites, operation)
	if waiter != nil {
		delete(waiter.state.writeWaiters, operation)
	}
}

// TryUDPSend queues a datagram for peer.
func (reactor *Reactor) TryUDPSend(handle uint64, data []byte, peer *net.UDPAddr) error {
	if peer == nil {
		return ErrInvalid
	}
	return reactor.TryUDPSendAddrPort(handle, data, udpAddrPort(peer))
}

// TryUDPSendAddrPort queues a datagram for peer without blocking.
func (reactor *Reactor) TryUDPSendAddrPort(handle uint64, data []byte, peer netip.AddrPort) error {
	if !peer.IsValid() || len(data) > udpMaxDatagram {
		return ErrInvalid
	}
	reactor.mu.Lock()
	defer reactor.mu.Unlock()
	state, exists := reactor.datagrams[handle]
	if !exists {
		return ErrInvalid
	}
	if state.sendErr != nil {
		return state.sendErr
	}
	if len(state.sendQueue) >= cap(state.sendQueue) {
		return ErrWouldBlock
	}
	state.sendQueue <- udpSend{
		data: append(getBuffer(len(data))[:0], data...),
		peer: peer,
	}
	return nil
}

func (reactor *Reactor) StartUDPSendReady(handle, operation uint64) error {
	reactor.mu.Lock()
	state, exists := reactor.datagrams[handle]
	if !exists {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if err := reactor.claimOperationLocked(operation, operationUDPWrite); err != nil {
		reactor.mu.Unlock()
		return err
	}
	waiter := &udpWriteWaiter{
		state: state,
		ready: len(state.sendQueue) < cap(state.sendQueue) || state.sendErr != nil,
	}
	reactor.udpWrites[operation] = waiter
	state.writeWaiters[operation] = waiter
	ready := waiter.ready
	reactor.mu.Unlock()
	if ready {
		reactor.signalCompletion()
	}
	return nil
}

func (reactor *Reactor) TakeUDPSendReady(operation uint64) error {
	reactor.mu.Lock()
	waiter, exists := reactor.udpWrites[operation]
	if !exists || reactor.operations[operation] != operationUDPWrite {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if !waiter.ready {
		reactor.mu.Unlock()
		return ErrPending
	}
	reactor.forgetUDPWriteLocked(operation, waiter)
	reactor.releaseOperationLocked(operation, operationUDPWrite)
	if _, registered := reactor.datagramHandleLocked(waiter.state); !registered {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	err := waiter.state.sendErr
	reactor.mu.Unlock()
	return err
}

func udpAddrPort(address *net.UDPAddr) netip.AddrPort {
	ip, _ := netip.AddrFromSlice(address.IP)
	if address.Zone != "" {
		ip = ip.WithZone(address.Zone)
	}
	return netip.AddrPortFrom(ip, uint16(address.Port))
}
