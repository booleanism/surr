package coder

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

// aacDecCandidates is the AAC decoder preference order, best first.
//
// fdkaacdec is the best of the three and was the hardcoded default, but Debian
// builds no fdk-aac plugin, so it cannot be assumed. faad is next (libgstfaad.so,
// in Debian's gstreamer1.0-plugins-bad) and avdec_aac (gstreamer1.0-libav) is the
// last resort. All three take the same AAC caps and emit the same raw audio, so
// the rest of the pipeline does not care which one is picked.
var aacDecCandidates = []string{"fdkaacdec", "faad", "avdec_aac"}

type aacdec struct {
	*coder

	aacparse gst.Element
	dec      gst.Element
	decName  string
}

func NewAACDec() *aacdec {
	a := &aacdec{
		coder: &coder{
			Bin: gst.NewBin(fmt.Sprintf("aacdec-%d", coderSeq.Add(1))).(gst.Bin),
		},
	}

	a.aacparse = gst.ElementFactoryMake("aacparse", "aacparse")
	a.dec, a.decName = pickElement(aacDecCandidates...)

	if !a.Valid() {
		return a
	}

	a.AddMany(a.aacparse, a.dec)
	a.aacparse.Link(a.dec)

	src := a.dec.GetStaticPad("src")
	a.AddPad(gst.NewGhostPad("src", src))

	sink := a.aacparse.GetStaticPad("sink")
	a.AddPad(gst.NewGhostPad("sink", sink))

	return a
}

// Valid reports whether every element was actually created.
//
// ElementFactoryMake returns nil for a factory that is not installed, and
// linking through a nil element crashes the process; callers turn this into a
// startup error instead.
func (a *aacdec) Valid() bool {
	return a.aacparse != nil && a.dec != nil
}

// ElementName is the decoder the fallback settled on, for the log line that
// records which one is in use.
func (a *aacdec) ElementName() string {
	return a.decName
}
