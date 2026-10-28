package hostabi

import (
	"net"
	"net/netip"
	"testing"
)

func TestUDPPeerMetadataMatchesNetIPCodec(t *testing.T) {
	for _, text := range []string{"192.0.2.7:53", "[::ffff:192.0.2.7]:53", "[2001:db8::1]:4500", "[::]:0"} {
		peer := netip.MustParseAddrPort(text)
		fast, err := encodeUDPPeerMetadata(peer)
		if err != nil {
			t.Fatalf("%s: encode: %v", text, err)
		}
		reference, err := encodeUDPMetadata(net.UDPAddrFromAddrPort(peer), nil, 0)
		if err != nil {
			t.Fatalf("%s: reference encode: %v", text, err)
		}
		if fast != reference {
			t.Fatalf("%s: metadata %x, want %x", text, fast, reference)
		}
		decoded, err := decodeUDPPeerMetadata(fast[:])
		if err != nil {
			t.Fatalf("%s: decode: %v", text, err)
		}
		if decoded != netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port()) {
			t.Fatalf("%s: decoded %s", text, decoded)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			metadata, _ := encodeUDPPeerMetadata(peer)
			_, _ = decodeUDPPeerMetadata(metadata[:])
		}); allocations != 0 {
			t.Fatalf("%s: %.1f allocations", text, allocations)
		}
	}
	// try_udp_send rejects optional fields, as the reference path did.
	withSource, err := encodeUDPMetadata(&net.UDPAddr{IP: net.IPv4(192, 0, 2, 7), Port: 53}, net.IPv4(198, 51, 100, 1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeUDPPeerMetadata(withSource[:]); err == nil {
		t.Fatal("metadata with a source address was accepted")
	}
	flowinfo, _ := encodeUDPPeerMetadata(netip.MustParseAddrPort("[2001:db8::1]:1"))
	flowinfo[20] = 1
	if _, err = decodeUDPPeerMetadata(flowinfo[:]); err == nil {
		t.Fatal("metadata with a flow label was accepted")
	}
}
