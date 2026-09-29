package rtp

import (
	"github.com/go-gst/go-gst/pkg/gst"
	"github.com/rs/zerolog"
)

type rtpmp4gdepay struct {
	*depayloader
}

func NewRTPMP4GDepay(pipe gst.Pipeline, log zerolog.Logger) (*gst.Caps, *rtpmp4gdepay) {
	r := &rtpmp4gdepay{
		depayloader: &depayloader{
			Bin:  gst.NewBin("rtpmp4gdepay").(gst.Bin),
			pipe: pipe,
			log:  log,
		},
	}

	r.queue = gst.ElementFactoryMake("queue", "queue")
	r.depay = gst.ElementFactoryMake("rtpmp4gdepay", "rtpmp4gdepay")

	r.AddMany(r.queue, r.depay)

	sinkPad := r.queue.GetStaticPad("sink")
	r.AddPad(gst.NewGhostPad("sink", sinkPad))

	srcPad := r.depay.GetStaticPad("src")
	r.AddPad(gst.NewGhostPad("src", srcPad))

	return gst.CapsFromString("application/x-rtp,encoding-name=MPEG4-GENERIC"), r
}
