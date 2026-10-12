package main

import "testing"

func TestSRTHostParsesIPv4(t *testing.T) {
	c := &CommandLine{}

	got, err := c.srtHost("127.0.0.1:4200")
	if err != nil {
		t.Fatalf("srtHost: %v", err)
	}

	if got.Host != "127.0.0.1" || got.Port != 4200 {
		t.Fatalf("got %+v; want {127.0.0.1 4200}", got)
	}
}

// The previous split on ":" read "[::1]:4200" as host "[" and failed on the
// port, so no IPv6 listen address could be configured at all.
func TestSRTHostParsesBracketedIPv6(t *testing.T) {
	c := &CommandLine{}

	for _, tc := range []struct {
		in   string
		host string
		port uint16
	}{
		{"[::1]:4200", "::1", 4200},
		{"[2001:db8::1]:9000", "2001:db8::1", 9000},
		{"[::]:4201", "::", 4201},
	} {
		got, err := c.srtHost(tc.in)
		if err != nil {
			t.Fatalf("srtHost(%q): %v", tc.in, err)
		}

		if got.Host != tc.host || got.Port != tc.port {
			t.Fatalf("srtHost(%q) = %+v; want {%s %d}", tc.in, got, tc.host, tc.port)
		}
	}
}

func TestSRTHostRejectsMalformedAddresses(t *testing.T) {
	c := &CommandLine{}

	for _, in := range []string{
		"127.0.0.1",       // no port
		":4200",           // no host
		"127.0.0.1:http",  // not a number
		"127.0.0.1:70000", // past uint16
		"::1:4200",        // unbracketed IPv6 is ambiguous
	} {
		if _, err := c.srtHost(in); err == nil {
			t.Fatalf("srtHost(%q) was accepted", in)
		}
	}
}
