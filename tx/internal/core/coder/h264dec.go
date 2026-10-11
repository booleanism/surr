package coder

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type h264dec struct {
	*coder
	h264parse gst.Element
	dec       gst.Element
}

func NewH264Dec() *h264dec {
	h := &h264dec{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("h264dec-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	h.h264parse = gst.ElementFactoryMake("h264parse", "h264parse")
	h.dec = decElement()

	if !h.Valid() {
		return h
	}

	h.AddMany(h.h264parse, h.dec)
	h.h264parse.Link(h.dec)

	sink := h.h264parse.GetStaticPad("sink")
	h.AddPad(gst.NewGhostPad("sink", sink))
	src := h.dec.GetStaticPad("src")
	h.AddPad(gst.NewGhostPad("src", src))

	return h
}

// Valid reports whether every element was actually created.
//
// ElementFactoryMake returns nil for a factory that is not installed, and
// linking through a nil element crashes the process; callers turn this into a
// startup error instead. This is the decoder the producer pipeline always
// builds, so a missing one is a hard startup failure rather than a degraded
// stream.
func (h *h264dec) Valid() bool {
	return h.h264parse != nil && h.dec != nil
}
