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

	// auto-reconnect: the sink is the SRT egress to rx. With it off, an rx
	// restart (or any transient network error) ends the stream and the bus
	// error tears down the whole tx process.
	srtsink := gst.ElementFactoryMakeWithProperties("srtsink", map[string]any{
		"auto-reconnect": true,
		"uri":            uri,
		"mode":           int32(1),
	})
	s.Add(srtsink)

	sink := srtsink.GetStaticPad("sink")
	s.AddPad(gst.NewGhostPad("sink", sink))

	return s
}
