package coder

import (
	"fmt"

	"github.com/booleanism/surr/tx/internal/core"
	"github.com/go-gst/go-gst/pkg/gst"
)

type h264enc struct {
	*coder

	videoscale gst.Element
	capsfilter gst.Element
	h264enc    gst.Element
	h264parse  gst.Element
}

func NewH264Enc(specs *core.StreamSpecs) *h264enc {
	e := &h264enc{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("h264enc-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	e.videoscale = gst.ElementFactoryMake("videoscale", "videoscale")
	caps := gst.CapsFromString("video/x-raw")
	if specs != nil && specs.Width > 0 && specs.Height > 0 {
		caps = gst.CapsFromString(fmt.Sprintf("video/x-raw, width=%d, height=%d", specs.Width, specs.Height))
	}

	e.capsfilter = gst.ElementFactoryMakeWithProperties("capsfilter", map[string]any{
		"name": "capsfilter",
		"caps": caps,
	})

	e.h264enc = encElement()
	if specs != nil {
		e.h264enc = encElementWithOptions(specs.Bitrate)
	}

	e.h264parse = gst.ElementFactoryMake("h264parse", "h264parse")

	e.AddMany(e.videoscale, e.capsfilter, e.h264enc, e.h264parse)
	gst.LinkMany(e.videoscale, e.capsfilter, e.h264enc, e.h264parse)

	sink := e.videoscale.GetStaticPad("sink")
	e.AddPad(gst.NewGhostPad("sink", sink))

	src := e.h264parse.GetStaticPad("src")
	e.AddPad(gst.NewGhostPad("src", src))

	return e
}
