package rtp

import (
	"github.com/go-gst/go-gst/pkg/gst"
)

type rtpmp4adepay struct {
	*depayloader
}

func NewRTPMP4ADepay(pipe gst.Pipeline) (*gst.Caps, *rtpmp4adepay) {
	r := &rtpmp4adepay{
		depayloader: &depayloader{
			Bin:  gst.NewBin("rtpmp4adepay").(gst.Bin),
			pipe: pipe,
		},
	}

	r.queue = gst.ElementFactoryMake("queue", "queue")
	r.depay = gst.ElementFactoryMake("rtpmp4adepay", "rtpmp4adepay")

	r.AddMany(r.queue, r.depay)

	sinkPad := r.queue.GetStaticPad("sink")
	r.AddPad(gst.NewGhostPad("sink", sinkPad))

	srcPad := r.depay.GetStaticPad("src")
	r.AddPad(gst.NewGhostPad("src", srcPad))

	return gst.CapsFromString("application/x-rtp,encoding-name=MP4A-LATM"), r
}
