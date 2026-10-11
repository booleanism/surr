package coder

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

// aacEncCandidates is the AAC encoder preference order, best first.
//
// Same reasoning as aacDecCandidates: fdkaacenc is the best and was the
// hardcoded default, but no Debian package provides it, so voaacenc
// (libgstvoaac.so, in Debian's gstreamer1.0-plugins-bad) and avenc_aac
// (gstreamer1.0-libav) are the fallbacks. All three take the same raw audio
// caps and emit AAC, which is what the DASH muxer downstream expects.
var aacEncCandidates = []string{"fdkaacenc", "voaacenc", "avenc_aac"}

type aacenc struct {
	*coder

	enc      gst.Element
	aacparse gst.Element
	encName  string
}

func NewAACEnc() *aacenc {
	e := &aacenc{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("aacenc-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	e.enc, e.encName = pickElement(aacEncCandidates...)
	e.aacparse = gst.ElementFactoryMake("aacparse", "aacparse")

	if !e.Valid() {
		return e
	}

	e.AddMany(e.enc, e.aacparse)

	e.enc.Link(e.aacparse)

	sink := e.enc.GetStaticPad("sink")
	e.AddPad(gst.NewGhostPad("sink", sink))

	src := e.aacparse.GetStaticPad("src")
	e.AddPad(gst.NewGhostPad("src", src))

	return e
}

// Valid reports whether every element was actually created.
//
// ElementFactoryMake returns nil for a factory that is not installed, and
// linking through a nil element crashes the process; callers turn this into a
// startup error instead.
func (e *aacenc) Valid() bool {
	return e.enc != nil && e.aacparse != nil
}

// ElementName is the encoder the fallback settled on, for the log line that
// records which one is in use.
func (e *aacenc) ElementName() string {
	return e.encName
}
