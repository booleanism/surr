package rtp

import (
	"github.com/go-gst/go-gst/pkg/gst"
	"github.com/rs/zerolog"
)

type rtph264depay struct {
	*depayloader
}

func NewRTPH264Depay(pipe gst.Pipeline, log zerolog.Logger) (*gst.Caps, *rtph264depay) {
	r := &rtph264depay{
		depayloader: &depayloader{
			Bin:  gst.NewBin("rtph264depay").(gst.Bin),
			pipe: pipe,
			log:  log,
		},
	}

	r.queue = gst.ElementFactoryMake("queue", "queue")
	r.depay = gst.ElementFactoryMake("rtph264depay", "rtph264depay")

	r.AddMany(r.queue, r.depay)

	sinkPad := r.queue.GetStaticPad("sink")
	r.AddPad(gst.NewGhostPad("sink", sinkPad))

	srcPad := r.depay.GetStaticPad("src")
	r.AddPad(gst.NewGhostPad("src", srcPad))

	return gst.CapsFromString("application/x-rtp,encoding-name=H264"), r
}
