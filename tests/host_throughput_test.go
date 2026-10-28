package host_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	corehost "github.com/easytier/easytier/easytier-go"
	"github.com/easytier/easytier/easytier-go/platform"
)

// connectedPair starts two instances of one host joined over a loopback TCP
// tunnel and waits until a packet crosses it.
func connectedPair(tb testing.TB, ctx context.Context) (client, server *corehost.Instance) {
	tb.Helper()
	sockets := &recordingSocketFactory{}
	host, err := corehost.New(ctx, corehost.Options{
		Platform:            platform.Services{Sockets: sockets},
		PacketQueueCapacity: 1024,
	})
	if err != nil {
		tb.Fatalf("create host: %v", err)
	}
	tb.Cleanup(func() { _ = host.Close(context.Background()) })
	server, err = host.CreateInstance(ctx, instanceConfig(tb, 1, "10.144.0.1", 0, false, true))
	if err != nil {
		tb.Fatalf("create server: %v", err)
	}
	if err = server.Start(ctx); err != nil {
		tb.Fatalf("start server: %v", err)
	}
	client, err = host.CreateInstance(ctx, instanceConfig(tb, 2, "10.144.0.2", sockets.listenerPort(tb), true, false))
	if err != nil {
		tb.Fatalf("create client: %v", err)
	}
	if err = client.Start(ctx); err != nil {
		tb.Fatalf("start client: %v", err)
	}
	probe := ipv4Packet(net.IPv4(10, 144, 0, 2), net.IPv4(10, 144, 0, 1), []byte("probe"))
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err = client.SendPacket(ctx, probe); err != nil {
			tb.Fatalf("send probe: %v", err)
		}
		receiveCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err = server.ReceivePacket(receiveCtx)
		cancel()
		if err == nil {
			return client, server
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			tb.Fatalf("receive probe: %v", err)
		}
	}
	tb.Fatal("tunnel did not come up")
	return nil, nil
}

// BenchmarkPublicPacketThroughput pushes 1200-byte UDP datagrams through the
// TUN packet plane of one instance and counts what the peer delivers.
func BenchmarkPublicPacketThroughput(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client, server := connectedPair(b, ctx)
	packet := ipv4Packet(net.IPv4(10, 144, 0, 2), net.IPv4(10, 144, 0, 1), make([]byte, 1200))
	var delivered atomic.Int64
	receiveCtx, stopReceive := context.WithCancel(ctx)
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		for {
			if _, err := server.ReceivePacket(receiveCtx); err != nil {
				return
			}
			delivered.Add(1)
		}
	}()
	b.SetBytes(int64(len(packet)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := client.SendPacket(ctx, packet); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	time.Sleep(200 * time.Millisecond)
	stopReceive()
	<-receiverDone
	b.ReportMetric(float64(delivered.Load())/float64(b.N)*100, "%delivered")
}

// BenchmarkPublicPacketThroughputBatch is BenchmarkPublicPacketThroughput
// with SendPackets batches of 32 and ReceivePackets with ReleasePacket.
func BenchmarkPublicPacketThroughputBatch(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client, server := connectedPair(b, ctx)
	packet := ipv4Packet(net.IPv4(10, 144, 0, 2), net.IPv4(10, 144, 0, 1), make([]byte, 1200))
	batch := make([][]byte, 32)
	for index := range batch {
		batch[index] = packet
	}
	var delivered atomic.Int64
	receiveCtx, stopReceive := context.WithCancel(ctx)
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		received := make([][]byte, 64)
		for {
			n, err := server.ReceivePackets(receiveCtx, received)
			if err != nil {
				return
			}
			for _, packet := range received[:n] {
				corehost.ReleasePacket(packet)
			}
			delivered.Add(int64(n))
		}
	}()
	b.SetBytes(int64(len(packet) * len(batch)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := client.SendPackets(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	time.Sleep(200 * time.Millisecond)
	stopReceive()
	<-receiverDone
	b.ReportMetric(float64(delivered.Load())/float64(b.N*len(batch))*100, "%delivered")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(batch)), "ns/packet")
}

func TestPublicPacketBatchesArriveIntact(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, server := connectedPair(t, ctx)
	const count = 48
	batch := make([][]byte, count)
	for index := range batch {
		payload := make([]byte, 64+index)
		for offset := range payload {
			payload[offset] = byte(index)
		}
		batch[index] = ipv4Packet(net.IPv4(10, 144, 0, 2), net.IPv4(10, 144, 0, 1), payload)
	}
	if err := client.SendPackets(ctx, batch); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	seen := make(map[int]bool)
	received := make([][]byte, 16)
	for len(seen) < count {
		receiveCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		n, err := server.ReceivePackets(receiveCtx, received)
		stop()
		if err != nil {
			t.Fatalf("receive after %d packets: %v", len(seen), err)
		}
		for _, packet := range received[:n] {
			// A probe of connectedPair may still arrive late.
			if string(packet[20:]) == "probe" {
				corehost.ReleasePacket(packet)
				continue
			}
			index := len(packet) - 20 - 64
			if index < 0 || index >= count || string(packet) != string(batch[index]) {
				t.Fatalf("unexpected packet of %d bytes", len(packet))
			}
			seen[index] = true
			corehost.ReleasePacket(packet)
		}
	}
}
