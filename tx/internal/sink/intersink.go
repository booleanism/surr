package sink

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type intersink struct {
	*sink
}

func NewInterSink(prodName string) *intersink {
	s := &intersink{
		sink: &sink{
			Bin: gst.NewBin(fmt.Sprintf("intersink-%d", sinkSeq.Add(1))).(gst.Bin),
		},
	}

	intersink := gst.ElementFactoryMakeWithProperties("intersink", map[string]any{
		"name":          "intersink",
		"producer-name": prodName,
	})
	s.Add(intersink)

	sink := intersink.GetStaticPad("sink")
	s.AddPad(gst.NewGhostPad("sink", sink))

	return s
}
