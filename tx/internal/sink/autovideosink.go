package sink

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type autovideosink struct {
	*sink
}

func NewVideoSink() *autovideosink {
	s := &autovideosink{
		sink: &sink{
			Bin: gst.NewBin(fmt.Sprintf("autovideosink-%d", sinkSeq.Add(1))).(gst.Bin),
		},
	}

	vidconvert := gst.ElementFactoryMake("videoconvert", "videoconvert")
	vidsink := gst.ElementFactoryMake("autovideosink", "autovideosink")
	s.AddMany(vidconvert, vidsink)
	vidconvert.Link(vidsink)

	sink := vidconvert.GetStaticPad("sink")
	s.AddPad(gst.NewGhostPad("sink", sink))

	return s
}
