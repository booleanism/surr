package coder

import (
	"sync/atomic"

	"github.com/go-gst/go-gst/pkg/gst"
)

var coderSeq atomic.Uint32

type coder struct {
	gst.Bin
}

func (d *coder) LinkChain(pipe gst.Pipeline, linkFunc func(srcpad gst.Pad) gst.PadLinkReturn) func(srcpad gst.Pad) gst.PadLinkReturn {
	pipe.Add(d)
	return func(srcpad gst.Pad) gst.PadLinkReturn {
		sink := d.GetStaticPad("sink")
		ret := srcpad.Link(sink)
		if ret != gst.PadLinkOK {
			return ret
		}

		return linkFunc(d.GetStaticPad("src"))
	}
}
