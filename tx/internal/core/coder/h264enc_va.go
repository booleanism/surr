//go:build va

package coder

import "github.com/go-gst/go-gst/pkg/gst"

func encElement() gst.Element {
	return gst.ElementFactoryMake("vah264enc", "vah264enc")
}

func encElementWithOptions(bitrate uint64) gst.Element {
	return gst.ElementFactoryMakeWithProperties("vah264enc", map[string]any{
		"name":              "vah264enc",
		"bitrate":           bitrate,
		"rate-control":      uint32(2),
		"target-percentage": uint32(50),
		"key-int-max":       uint32(120),
		"b-frames":          uint32(3),
		"b-pyramid":         true,
		"trellis":           true,
		"target-usage":      uint32(1),
	})
}
