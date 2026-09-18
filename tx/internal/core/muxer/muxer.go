package muxer

import (
	"sync/atomic"

	"github.com/go-gst/go-gst/pkg/gst"
)

var muxerSeq atomic.Uint32

type muxer struct {
	gst.Bin

	mux gst.Element

	videoPadTemplate string
	audioPadTemplate string
}

func (q *muxer) VideoSink() func(srcpad gst.Pad) gst.PadLinkReturn {
	return q.req(q.videoPadTemplate)
}

func (q *muxer) AudioSink() func(srcpad gst.Pad) gst.PadLinkReturn {
	return q.req(q.audioPadTemplate)
}

func (q *muxer) req(t string) func(srcpad gst.Pad) gst.PadLinkReturn {
	return func(srcpad gst.Pad) gst.PadLinkReturn {
		internalSink := q.mux.RequestPadSimple(t)
		if internalSink == nil {
			return gst.PadLinkNoformat
		}

		ghostPad := gst.NewGhostPad(internalSink.GetName(), internalSink)
		ghostPad.SetActive(true)
		q.AddPad(ghostPad)

		return srcpad.Link(ghostPad)
	}
}
