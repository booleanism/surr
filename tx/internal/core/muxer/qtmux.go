package muxer

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type qtmux struct {
	*muxer
}

func NewQTMux(pipe gst.Pipeline) *qtmux {
	q := &qtmux{
		muxer: &muxer{
			Bin:              gst.NewBin(fmt.Sprintf("qtmux-%d", muxerSeq.Add(1))).(gst.Bin),
			videoPadTemplate: "video_%u",
			audioPadTemplate: "audio_%u",
		},
	}

	q.mux = gst.ElementFactoryMake("qtmux", "mux")
	q.Add(q.mux)

	src := q.mux.GetStaticPad("src")
	q.AddPad(gst.NewGhostPad("src", src))

	return q
}
