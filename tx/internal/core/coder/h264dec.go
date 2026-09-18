package coder

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type h264dec struct {
	*coder
	h264parse gst.Element
	vah264dec gst.Element
}

func NewH264Dec() *h264dec {
	h := &h264dec{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("h264dec-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	h.h264parse = gst.ElementFactoryMake("h264parse", "h264parse")
	h.vah264dec = decElement()

	h.AddMany(h.h264parse, h.vah264dec)
	h.h264parse.Link(h.vah264dec)

	sink := h.h264parse.GetStaticPad("sink")
	h.AddPad(gst.NewGhostPad("sink", sink))
	src := h.vah264dec.GetStaticPad("src")
	h.AddPad(gst.NewGhostPad("src", src))

	return h
}
