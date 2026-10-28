package reactor

import "testing"

func TestBufferPoolRoundTripDoesNotAllocate(t *testing.T) {
	for _, size := range []int{1, smallBufferSize, smallBufferSize + 1, largeBufferSize} {
		putBuffer(getBuffer(size))
		allocations := testing.AllocsPerRun(100, func() {
			buffer := getBuffer(size)
			if len(buffer) != size {
				t.Fatalf("buffer length = %d, want %d", len(buffer), size)
			}
			putBuffer(buffer)
		})
		if allocations != 0 {
			t.Fatalf("size %d: %.1f allocations per round trip", size, allocations)
		}
	}
	if buffer := getBuffer(largeBufferSize + 1); cap(buffer) != largeBufferSize+1 {
		t.Fatalf("oversized buffer capacity = %d", cap(buffer))
	}
}
