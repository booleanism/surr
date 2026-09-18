package src

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type rtspSrc struct {
	gst.Bin
}

func NewRTSPSrc(uri string, p PadAddedCallback) *rtspSrc {
	r := &rtspSrc{
		Bin: gst.NewBin(fmt.Sprintf("rtspsrc-%d", srcSeq.Add(1))).(gst.Bin),
	}

	rtspsrc := gst.ElementFactoryMakeWithProperties("rtspsrc", map[string]any{
		"name":     "src",
		"location": uri,
	})
	r.Add(rtspsrc)

	rtspsrc.Connect("pad-added", r.setup(p))

	return r
}

func (r *rtspSrc) setup(cb PadAddedCallback) func(self gst.Element, pad gst.Pad) {
	return func(self gst.Element, pad gst.Pad) {
		if pad.IsLinked() {
			return
		}

		caps := pad.GetCurrentCaps()
		if caps == nil {
			return
		}

		for c, call := range cb {
			if !caps.CanIntersect(c) {
				continue
			}

			ghost := gst.NewGhostPad(pad.GetName(), pad)
			r.AddPad(ghost)
			ghost.SetActive(true)

			if call(self, ghost) {
				return
			}

			r.RemovePad(ghost) // cleanup if this callback didn't take it
		}
	}
}
