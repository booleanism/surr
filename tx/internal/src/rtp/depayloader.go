package rtp

import (
	"github.com/go-gst/go-gst/pkg/gst"
	"github.com/rs/zerolog"
)

type depayloader struct {
	gst.Bin

	queue gst.Element
	depay gst.Element

	linkFunc func(srcpad gst.Pad) gst.PadLinkReturn
	pipe     gst.Pipeline
	log      zerolog.Logger
}

func (r *depayloader) linkChain(linkFunc func(src gst.Pad) gst.PadLinkReturn) {
	r.pipe.AddMany(r)
	r.linkFunc = func(src gst.Pad) gst.PadLinkReturn {
		ret := src.Link(r.GetStaticPad("sink"))
		if ret != gst.PadLinkOK {
			return ret
		}

		ret = r.queue.GetStaticPad("src").Link(r.depay.GetStaticPad("sink"))
		if ret != gst.PadLinkOK {
			return ret
		}

		return linkFunc(r.GetStaticPad("src"))
	}
}

func (r *depayloader) OnPadAdded(linkFunc func(src gst.Pad) gst.PadLinkReturn) func(self gst.Element, pad gst.Pad) bool {
	r.linkChain(linkFunc)
	return func(self gst.Element, pad gst.Pad) bool {
		if r.linkFunc != nil {
			ret := r.linkFunc(pad)
			if ret != gst.PadLinkOK {
				r.log.Warn().Str("reason", ret.String()).Msg("unable to link pad")
				return false
			}
		}

		return true
	}
}
