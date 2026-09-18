//go:build !va

package coder

import "github.com/go-gst/go-gst/pkg/gst"

func encElement() gst.Element {
	return gst.ElementFactoryMake("openh264enc", "openh264enc")
}

func encElementWithOptions(bitrate uint64) gst.Element {
	return gst.ElementFactoryMakeWithProperties("openh264enc", map[string]any{
		"name":         "openh264enc",
		"bitrate":      uint32(bitrate),
		"rate-control": uint32(1),
		"gop-size":     uint32(120),
		"complexity":   uint32(2),
	})
}
