package coder

import (
	"fmt"

	"github.com/go-gst/go-gst/pkg/gst"
)

// pickElement returns the first of candidates this runtime actually has, along
// with its name, or a nil element and an empty name when none of them are
// installed.
//
// Which factories exist is a property of the distribution, not of the source:
// the same tx binary finds fdkaacdec on a host that has deb-multimedia's
// GStreamer and does not find it on plain Debian, which ships no fdk-aac plugin
// at all (fdk-aac is non-free, so Debian never builds it). Hardcoding one
// factory therefore makes the binary depend on how the machine was set up.
// Asking the registry in preference order removes that.
//
// ElementFactoryMake returns a nil interface for a factory that is not
// installed, and every later call on a nil element -- Add, Link, GetStaticPad --
// crashes the process, so callers must check the result before using it.
func pickElement(candidates ...string) (gst.Element, string) {
	for _, name := range candidates {
		if el := gst.ElementFactoryMake(name, name); el != nil {
			return el, name
		}
	}

	return nil, ""
}

// CheckDecoders reports a startup error when the elements the producer
// pipeline's two decoders need are missing from this GStreamer installation.
//
// It lives here rather than at the call site because the caller cannot name
// these types -- they are unexported on purpose -- and because the two checks
// belong together: both have to happen before anything is wired up, since
// linking through the nil element ElementFactoryMake returns crashes the
// process rather than degrading the stream.
func CheckDecoders(vid *h264dec, aud *aacdec) error {
	if !vid.Valid() {
		return fmt.Errorf("h264 decoder elements unavailable: install h264parse and openh264dec (gstreamer1.0-plugins-bad), or build with -tags va for vah264dec")
	}

	if !aud.Valid() {
		return fmt.Errorf("aac decoder elements unavailable: install aacparse (gstreamer1.0-plugins-bad) and one of fdkaacdec, faad, avdec_aac")
	}

	return nil
}
