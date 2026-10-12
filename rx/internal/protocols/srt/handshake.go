package srt

import (
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/haivision/srtgo"
	"github.com/rs/zerolog"
)

// handshakeLookupTimeout bounds the broker round trip inside an SRT listen
// callback.
//
// The callback runs on libsrt's listener thread, so a lookup that never returns
// stops every other handshake on that socket -- publishers and subscribers
// alike -- with nothing to show for it. libsrt offers no way to move the work
// off that thread, so a timeout is the only thing that keeps one wedged RPC
// from parking the whole listener; each handshake is still serialized behind
// the one before it.
const handshakeLookupTimeout = 5 * time.Second

// rejection describes a refused SRT handshake.
//
// The reason is set on the socket so the peer is told why rather than just
// seeing the connection disappear, and it is logged with the same wording, so
// the two cannot drift apart.
//
// Field order is not free: cause is the only interface field, and govet's
// fieldalignment wants it ahead of the three strings so the GC's scan prefix
// stops at 56 bytes rather than 64.
type rejection struct {
	cause    error
	msg      string
	streamid string
	reason   string
	nodeID   uuid.UUID
	code     int
}

// rejectHandshake sets the SRT rejection reason the peer will receive and logs
// the refusal.
func rejectHandshake(log *zerolog.Logger, socket *srtgo.SrtSocket, addr *net.UDPAddr, r rejection) {
	if err := socket.SetRejectReason(r.code); err != nil {
		log.Error().Err(err).Str("reason", r.reason).Msg("set rejection reason")
	}

	ev := log.Warn().
		Str("peer", addr.String()).
		Str("streamid", r.streamid).
		Str("reason", r.reason)

	if r.nodeID != uuid.Nil {
		ev = ev.Str("node_id", r.nodeID.String())
	}

	if r.cause != nil {
		ev = ev.Err(r.cause)
	}

	ev.Msg(r.msg)
}
