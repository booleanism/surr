//go:build !va

package coder

import "github.com/go-gst/go-gst/pkg/gst"

func decElement() gst.Element {
	return gst.ElementFactoryMake("openh264dec", "openh264dec")
}
