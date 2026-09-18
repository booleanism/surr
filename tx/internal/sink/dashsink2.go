package sink

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type dashsink2 struct {
	gst.Bin

	sink gst.Element
}

func NewDashSink2(props map[string]any) *dashsink2 {
	d := &dashsink2{
		Bin: gst.NewBin(fmt.Sprintf("dashsink2_%d", sinkSeq.Add(1))).(gst.Bin),
	}

	d.sink = gst.ElementFactoryMakeWithProperties("dashsink2", props)
	d.Add(d.sink)

	return d
}

func (d *dashsink2) VideoSink() func(srcpad gst.Pad) gst.PadLinkReturn {
	return d.req("video_%u")
}

func (d *dashsink2) AudioSink() func(srcpad gst.Pad) gst.PadLinkReturn {
	return d.req("audio_%u")
}

func (d *dashsink2) req(t string) func(srcpad gst.Pad) gst.PadLinkReturn {
	return func(srcpad gst.Pad) gst.PadLinkReturn {
		internalSink := d.sink.RequestPadSimple(t)
		if internalSink == nil {
			return gst.PadLinkNoformat
		}

		ghostPad := gst.NewGhostPad(internalSink.GetName(), internalSink)
		ghostPad.SetActive(true)
		d.AddPad(ghostPad)

		ret := srcpad.Link(ghostPad)
		if ret != gst.PadLinkOK {
			d.RemovePad(ghostPad)
			d.sink.ReleaseRequestPad(internalSink)
		}
		return ret
	}
}
