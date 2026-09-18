package coder

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type aacdec struct {
	*coder

	aacparse gst.Element
	faad     gst.Element
}

func NewAACDec() *aacdec {
	a := &aacdec{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("aacdec-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	a.aacparse = gst.ElementFactoryMake("aacparse", "aacparse")
	a.faad = gst.ElementFactoryMake("fdkaacdec", "fdkaacdec")

	a.AddMany(a.aacparse, a.faad)
	a.aacparse.Link(a.faad)

	src := a.faad.GetStaticPad("src")
	a.AddPad(gst.NewGhostPad("src", src))

	sink := a.aacparse.GetStaticPad("sink")
	a.AddPad(gst.NewGhostPad("sink", sink))

	return a
}
