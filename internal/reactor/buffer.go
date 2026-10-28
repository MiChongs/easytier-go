package reactor

import "sync"

// Buffers carry payloads between host sockets and guest memory. They are
// pooled by size class: a packet or datagram fits the small class, stream
// reads and maximum-size datagrams the larger ones. The pools hold array
// pointers, which a full-capacity slice converts back to without allocating.
const (
	smallBufferSize  = 2 << 10
	mediumBufferSize = 16 << 10
	largeBufferSize  = 64 << 10
)

var (
	smallBuffers  = sync.Pool{New: func() any { return new([smallBufferSize]byte) }}
	mediumBuffers = sync.Pool{New: func() any { return new([mediumBufferSize]byte) }}
	largeBuffers  = sync.Pool{New: func() any { return new([largeBufferSize]byte) }}
)

// getBuffer returns a buffer of length size, pooled when a class holds it.
func getBuffer(size int) []byte {
	switch {
	case size <= smallBufferSize:
		return smallBuffers.Get().(*[smallBufferSize]byte)[:size]
	case size <= mediumBufferSize:
		return mediumBuffers.Get().(*[mediumBufferSize]byte)[:size]
	case size <= largeBufferSize:
		return largeBuffers.Get().(*[largeBufferSize]byte)[:size]
	default:
		return make([]byte, size)
	}
}

// putBuffer returns a buffer from getBuffer to its pool. Buffers of other
// capacities are left to the garbage collector.
func putBuffer(buffer []byte) {
	switch cap(buffer) {
	case smallBufferSize:
		smallBuffers.Put((*[smallBufferSize]byte)(buffer[:smallBufferSize]))
	case mediumBufferSize:
		mediumBuffers.Put((*[mediumBufferSize]byte)(buffer[:mediumBufferSize]))
	case largeBufferSize:
		largeBuffers.Put((*[largeBufferSize]byte)(buffer[:largeBufferSize]))
	}
}

// ReleasePacket returns a packet received from a packet sink to the pool. The
// caller must not use the packet afterwards. Releasing is optional.
func ReleasePacket(packet []byte) {
	putBuffer(packet)
}
