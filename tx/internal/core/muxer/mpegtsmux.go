package muxer

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type mpetsmux struct {
	*muxer
}

func NewMPEGTSMux() *mpetsmux {
	q := &mpetsmux{
		muxer: &muxer{
			Bin:              gst.NewBin(fmt.Sprintf("mpegtsmux-%d", muxerSeq.Add(1))).(gst.Bin),
			videoPadTemplate: "sink_%d",
			audioPadTemplate: "sink_%d",
		},
	}

	q.mux = gst.ElementFactoryMake("mpegtsmux", "mux")
	q.Add(q.mux)

	src := q.mux.GetStaticPad("src")
	q.AddPad(gst.NewGhostPad("src", src))

	return q
}
