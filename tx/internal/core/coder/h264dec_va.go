//go:build va

package coder

import "github.com/go-gst/go-gst/pkg/gst"

func decElement() gst.Element {
	return gst.ElementFactoryMake("vah264dec", "vah264dec")
}
