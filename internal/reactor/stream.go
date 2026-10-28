package reactor

import (
	"errors"
	"io"
	"net"
)

type streamState struct {
	connection net.Conn
	closed     bool

	readQueue   []*readOperation
	readRunning bool
	readReady   bool
	// readBuffer is the pooled buffer readData points into. It is held only
	// while read data waits for the guest, so idle streams hold none.
	readBuffer []byte
	readData   []byte
	readErr    error

	writeQueue   []*writeOperation
	writeRunning bool
	writeActive  *writeOperation
}

type readOperation struct {
	capacity uint32
	stream   *streamState
}

type writeOperation struct {
	operation uint64
	stream    *streamState
	data      []byte
	done      bool
	err       error
}

type streamReadTask struct {
	capacity uint32
	stream   *streamState
}

func newStreamState(connection net.Conn) *streamState {
	return &streamState{connection: connection}
}

func (task streamReadTask) launch(reactor *Reactor) {
	if task.stream != nil {
		go reactor.runStreamRead(task.capacity, task.stream)
	}
}

func (reactor *Reactor) StartRead(handle, operation uint64, capacity uint32) error {
	reactor.mu.Lock()
	stream, exists := reactor.streams[handle]
	if !exists || capacity == 0 {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if err := reactor.claimOperationLocked(operation, operationRead); err != nil {
		reactor.mu.Unlock()
		return err
	}
	waiter := &readOperation{
		capacity: capacity,
		stream:   stream,
	}
	reactor.reads[operation] = waiter
	stream.readQueue = append(stream.readQueue, waiter)
	ready := stream.readAvailableLocked()
	task := reactor.prepareStreamReadLocked(stream)
	reactor.mu.Unlock()

	task.launch(reactor)
	if ready {
		reactor.signalCompletion()
	}
	return nil
}

func (reactor *Reactor) prepareStreamReadLocked(stream *streamState) streamReadTask {
	if reactor.closed ||
		stream.closed ||
		stream.readRunning ||
		stream.readReady ||
		len(stream.readQueue) == 0 {
		return streamReadTask{}
	}
	stream.readRunning = true
	reactor.workers.Add(1)
	return streamReadTask{
		capacity: stream.readQueue[0].capacity,
		stream:   stream,
	}
}

func (reactor *Reactor) runStreamRead(
	capacity uint32,
	stream *streamState,
) {
	defer reactor.workers.Done()
	buffer := getBuffer(int(capacity))
	n, err := stream.connection.Read(buffer)

	reactor.mu.Lock()
	stream.readRunning = false
	if n > 0 {
		stream.readBuffer = buffer
		stream.readData = buffer[:n]
	} else {
		putBuffer(buffer)
	}
	stream.readErr = err
	stream.readReady = true
	ready := len(stream.readQueue) > 0
	reactor.mu.Unlock()
	if ready {
		reactor.signalCompletion()
	}
}

// TakeRead completes a read and returns a copy of its data.
func (reactor *Reactor) TakeRead(operation uint64) ([]byte, error) {
	var data []byte
	_, err := reactor.TakeReadInto(operation, func(read []byte) error {
		data = append([]byte(nil), read...)
		return nil
	})
	return data, err
}

// TakeReadInto completes a read by passing its data to deliver, which must
// copy it out before returning: the buffer is reused once deliver returns.
// It returns the number of bytes delivered. An error from deliver is
// returned after the read is consumed.
func (reactor *Reactor) TakeReadInto(operation uint64, deliver func([]byte) error) (int, error) {
	reactor.mu.Lock()
	waiter, exists := reactor.reads[operation]
	if !exists || reactor.operations[operation] != operationRead {
		reactor.mu.Unlock()
		return 0, ErrInvalid
	}
	stream := waiter.stream
	if len(stream.readQueue) == 0 || stream.readQueue[0] != waiter {
		reactor.mu.Unlock()
		return 0, ErrPending
	}
	if !stream.readAvailableLocked() {
		reactor.mu.Unlock()
		return 0, ErrPending
	}

	var (
		taken      int
		err        error
		deliverErr error
	)
	if stream.readReady {
		taken = min(len(stream.readData), int(waiter.capacity))
		// Deliver before the next read can reuse the buffer.
		deliverErr = deliver(stream.readData[:taken])
		if taken == len(stream.readData) {
			if stream.readBuffer != nil {
				putBuffer(stream.readBuffer)
				stream.readBuffer = nil
			}
			stream.readData = nil
			err = stream.readErr
			stream.readErr = nil
			stream.readReady = false
		} else {
			stream.readData = stream.readData[taken:]
		}
	} else {
		err = net.ErrClosed
	}

	stream.readQueue[0] = nil
	stream.readQueue = stream.readQueue[1:]
	if len(stream.readQueue) == 0 {
		stream.readQueue = nil
	}
	delete(reactor.reads, operation)
	reactor.releaseOperationLocked(operation, operationRead)
	ready := len(stream.readQueue) > 0 && stream.readAvailableLocked()
	task := reactor.prepareStreamReadLocked(stream)
	reactor.mu.Unlock()

	task.launch(reactor)
	if ready {
		reactor.signalCompletion()
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if deliverErr != nil {
		return taken, deliverErr
	}
	return taken, err
}

func (reactor *Reactor) cancelStreamReadLocked(
	operation uint64,
) (streamReadTask, bool) {
	waiter := reactor.reads[operation]
	delete(reactor.reads, operation)
	if waiter == nil {
		return streamReadTask{}, false
	}
	stream := waiter.stream
	wasFirst := len(stream.readQueue) > 0 && stream.readQueue[0] == waiter
	stream.readQueue = removeReadOperation(stream.readQueue, waiter)
	ready := wasFirst && len(stream.readQueue) > 0 && stream.readAvailableLocked()
	task := reactor.prepareStreamReadLocked(stream)
	return task, ready
}

func removeReadOperation(
	queue []*readOperation,
	operation *readOperation,
) []*readOperation {
	for index, queued := range queue {
		if queued != operation {
			continue
		}
		copy(queue[index:], queue[index+1:])
		queue[len(queue)-1] = nil
		queue = queue[:len(queue)-1]
		if len(queue) == 0 {
			return nil
		}
		return queue
	}
	return queue
}

func (stream *streamState) readAvailableLocked() bool {
	return stream.readReady || stream.closed && !stream.readRunning
}

func (reactor *Reactor) StartWrite(handle, operation uint64, data []byte) error {
	data = append(getBuffer(len(data))[:0], data...)
	reactor.mu.Lock()
	stream, exists := reactor.streams[handle]
	if !exists {
		reactor.mu.Unlock()
		putBuffer(data)
		return ErrInvalid
	}
	if err := reactor.claimOperationLocked(operation, operationWrite); err != nil {
		reactor.mu.Unlock()
		putBuffer(data)
		return err
	}
	request := &writeOperation{
		operation: operation,
		stream:    stream,
		data:      data,
	}
	reactor.writes[operation] = request
	stream.writeQueue = append(stream.writeQueue, request)
	startWorker := !stream.writeRunning
	if startWorker {
		stream.writeRunning = true
		reactor.workers.Add(1)
	}
	reactor.mu.Unlock()

	if startWorker {
		go reactor.runStreamWrites(stream)
	}
	return nil
}

func (reactor *Reactor) runStreamWrites(stream *streamState) {
	defer reactor.workers.Done()
	for {
		reactor.mu.Lock()
		if reactor.closed || len(stream.writeQueue) == 0 {
			stream.writeQueue = nil
			stream.writeRunning = false
			reactor.mu.Unlock()
			return
		}
		request := stream.writeQueue[0]
		stream.writeQueue[0] = nil
		stream.writeQueue = stream.writeQueue[1:]
		if len(stream.writeQueue) == 0 {
			stream.writeQueue = nil
		}
		stream.writeActive = request
		data := request.data
		reactor.mu.Unlock()

		written := 0
		var writeErr error
		for written < len(data) {
			n, err := stream.connection.Write(data[written:])
			written += n
			if err != nil {
				writeErr = err
				break
			}
			if n == 0 {
				writeErr = io.ErrShortWrite
				break
			}
		}

		putBuffer(data)
		reactor.mu.Lock()
		stream.writeActive = nil
		request.data = nil
		ready := reactor.writes[request.operation] == request &&
			reactor.operations[request.operation] == operationWrite
		if ready {
			request.err = writeErr
			request.done = true
		}
		reactor.mu.Unlock()
		if ready {
			reactor.signalCompletion()
		}
	}
}

func (reactor *Reactor) TakeWrite(operation uint64) error {
	reactor.mu.Lock()
	result, exists := reactor.writes[operation]
	if !exists || reactor.operations[operation] != operationWrite {
		reactor.mu.Unlock()
		return ErrInvalid
	}
	if !result.done {
		reactor.mu.Unlock()
		return ErrPending
	}
	delete(reactor.writes, operation)
	reactor.releaseOperationLocked(operation, operationWrite)
	err := result.err
	reactor.mu.Unlock()
	return err
}

func (reactor *Reactor) cancelStreamWriteLocked(operation uint64) {
	request := reactor.writes[operation]
	delete(reactor.writes, operation)
	if request == nil || request.stream.writeActive == request {
		return
	}
	request.stream.writeQueue = removeWriteOperation(request.stream.writeQueue, request)
	putBuffer(request.data)
	request.data = nil
}

func removeWriteOperation(
	queue []*writeOperation,
	operation *writeOperation,
) []*writeOperation {
	for index, queued := range queue {
		if queued != operation {
			continue
		}
		copy(queue[index:], queue[index+1:])
		queue[len(queue)-1] = nil
		queue = queue[:len(queue)-1]
		if len(queue) == 0 {
			return nil
		}
		return queue
	}
	return queue
}

func (reactor *Reactor) closeStreamLocked(stream *streamState) bool {
	stream.closed = true
	ready := len(stream.readQueue) > 0 && stream.readAvailableLocked()
	for _, request := range stream.writeQueue {
		putBuffer(request.data)
		request.data = nil
		if reactor.writes[request.operation] != request {
			continue
		}
		request.err = net.ErrClosed
		request.done = true
		ready = true
	}
	stream.writeQueue = nil
	return ready
}

func (stream *streamState) shutdownLocked() {
	stream.closed = true
	stream.readQueue = nil
	stream.writeQueue = nil
}
