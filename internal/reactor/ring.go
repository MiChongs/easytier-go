package reactor

// ring is a fixed-capacity FIFO queue.
type ring[T any] struct {
	items []T
	head  int
	count int
}

func newRing[T any](capacity int) ring[T] {
	return ring[T]{items: make([]T, capacity)}
}

func (r *ring[T]) len() int   { return r.count }
func (r *ring[T]) full() bool { return r.count == len(r.items) }

func (r *ring[T]) push(item T) {
	r.items[(r.head+r.count)%len(r.items)] = item
	r.count++
}

func (r *ring[T]) pop() T {
	var zero T
	item := r.items[r.head]
	r.items[r.head] = zero
	r.head = (r.head + 1) % len(r.items)
	r.count--
	return item
}
