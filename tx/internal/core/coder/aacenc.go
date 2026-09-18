package coder

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type aacenc struct {
	*coder

	faac     gst.Element
	aacparse gst.Element
}

func NewAACEnc() *aacenc {
	e := &aacenc{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("aacenc-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	e.faac = gst.ElementFactoryMake("fdkaacenc", "fdkaacenc")
	e.aacparse = gst.ElementFactoryMake("aacparse", "aacparse")

	e.AddMany(e.faac, e.aacparse)

	e.faac.Link(e.aacparse)

	sink := e.faac.GetStaticPad("sink")
	e.AddPad(gst.NewGhostPad("sink", sink))

	src := e.aacparse.GetStaticPad("src")
	e.AddPad(gst.NewGhostPad("src", src))

	return e
}
