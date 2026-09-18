package sink

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type srtsink struct {
	*sink
}

func NewSRTSink(uri string) *srtsink {
	s := &srtsink{
		sink: &sink{
			Bin: gst.NewBin(fmt.Sprintf("srtsink-%d", sinkSeq.Add(1))).(gst.Bin),
		},
	}

	srtsink := gst.ElementFactoryMakeWithProperties("srtsink", map[string]any{
		"auto-reconnect": false,
		"uri":            uri,
		"mode":           int32(1),
	})
	s.Add(srtsink)

	sink := srtsink.GetStaticPad("sink")
	s.AddPad(gst.NewGhostPad("sink", sink))

	return s
}
