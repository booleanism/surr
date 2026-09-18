package sink

import (
	"sync/atomic"

	"github.com/go-gst/go-gst/pkg/gst"
)

var sinkSeq atomic.Uint32

type sink struct {
	gst.Bin
}

func (s *sink) LinkChain(pipe gst.Pipeline) func(srcpad gst.Pad) gst.PadLinkReturn {
	pipe.Add(s)
	return func(srcpad gst.Pad) gst.PadLinkReturn {
		return srcpad.Link(s.GetStaticPad("sink"))
	}
}
