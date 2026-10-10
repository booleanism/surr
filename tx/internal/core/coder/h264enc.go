package coder

import (
	"fmt"

	"github.com/booleanism/surr/tx/internal/core"
	"github.com/go-gst/go-gst/pkg/gst"
)

type h264enc struct {
	*coder

	videoconvert gst.Element
	videoscale   gst.Element
	capsfilter   gst.Element
	h264enc      gst.Element
	h264parse    gst.Element
}

func NewH264Enc(specs *core.StreamSpecs) *h264enc {
	e := &h264enc{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("h264enc-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	// videoconvert first: openh264enc and vah264enc only accept I420, so a
	// camera handing over YUY2/NV12 used to fail to negotiate.
	e.videoconvert = gst.ElementFactoryMake("videoconvert", "videoconvert")
	e.videoscale = gst.ElementFactoryMake("videoscale", "videoscale")

	// format is pinned because both encoders require I420. width/height are
	// only pinned when the caller asked for a scale; framerate and
	// pixel-aspect-ratio are deliberately left unconstrained, because the
	// encoder templates already accept any and pinning them would force a
	// guessed value onto an unusual source.
	caps := gst.CapsFromString("video/x-raw, format=I420")
	if specs != nil && specs.Width > 0 && specs.Height > 0 {
		caps = gst.CapsFromString(fmt.Sprintf("video/x-raw, format=I420, width=%d, height=%d", specs.Width, specs.Height))
	}

	e.capsfilter = gst.ElementFactoryMakeWithProperties("capsfilter", map[string]any{
		"name": "capsfilter",
		"caps": caps,
	})

	e.h264enc = encElement()
	if specs != nil {
		e.h264enc = encElementWithOptions(specs.Bitrate)
	}

	// config-interval=-1 re-inserts SPS/PPS before every IDR frame. Without it
	// a subscriber that joins the SRT stream after the first keyframe has no
	// parameter sets and cannot decode anything until the next one.
	e.h264parse = gst.ElementFactoryMakeWithProperties("h264parse", map[string]any{
		"name":            "h264parse",
		"config-interval": int32(-1),
	})

	e.AddMany(e.videoconvert, e.videoscale, e.capsfilter, e.h264enc, e.h264parse)
	gst.LinkMany(e.videoconvert, e.videoscale, e.capsfilter, e.h264enc, e.h264parse)

	sink := e.videoconvert.GetStaticPad("sink")
	e.AddPad(gst.NewGhostPad("sink", sink))

	src := e.h264parse.GetStaticPad("src")
	e.AddPad(gst.NewGhostPad("src", src))

	return e
}

// Valid reports whether every element was actually created.
//
// ElementFactoryMake returns nil for a factory that is not installed, and
// linking through a nil element crashes the process; callers turn this into a
// startup error instead.
func (e *h264enc) Valid() bool {
	return e.videoconvert != nil && e.videoscale != nil && e.capsfilter != nil && e.h264enc != nil && e.h264parse != nil
}
