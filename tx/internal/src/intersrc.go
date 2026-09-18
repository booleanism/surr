package src

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

type intersrc struct {
	gst.Bin
}

func NewInterSrc(name string) *intersrc {
	s := &intersrc{
		Bin: gst.NewBin(fmt.Sprintf("intersrc-%d", srcSeq.Add(1))).(gst.Bin),
	}

	src := gst.ElementFactoryMakeWithProperties("intersrc", map[string]any{
		"producer-name": name,
	})

	s.Add(src)
	s.AddPad(gst.NewGhostPad("src", src.GetStaticPad("src")))

	return s
}
