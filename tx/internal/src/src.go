package src

import (
	"sync/atomic"

	"github.com/go-gst/go-gst/pkg/gst"
)

var srcSeq atomic.Uint32

type (
	PadAddedFunc     func(self gst.Element, pad gst.Pad) bool
	PadAddedCallback = map[*gst.Caps]PadAddedFunc
)
