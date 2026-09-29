package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-gst/go-gst/pkg/gst"
	"github.com/rs/zerolog"
)

type StreamSpecs struct {
	Width   uint64 `json:"width"`
	Height  uint64 `json:"height"`
	Bitrate uint64 `json:"bitrate"`
}

// IsZero checks zero every fields. return false if every fields non-zero value.
func (s StreamSpecs) IsZero() bool {
	return s.Width == 0 || s.Height == 0 || s.Bitrate == 0
}

type Pipeline struct {
	gst.Pipeline
	n string
}

func NewPipeline(n string) *Pipeline {
	return &Pipeline{
		Pipeline: gst.NewPipeline(n).(gst.Pipeline),
		n:        n,
	}
}

func (p *Pipeline) WatchBus(ctx context.Context) error {
	for msg := range p.GetBus().Messages(ctx) {
		switch msg.Type() {
		case gst.MessageError:
			gerr, err := msg.ParseError()
			if err != nil {
				return fmt.Errorf("%s: %s", strings.ReplaceAll(gerr, "\n", "\\n"), strings.ReplaceAll(err.Error(), "\n", "\\n"))
			}

			return fmt.Errorf("%s", strings.ReplaceAll(gerr, "\n", "\\n"))
		case gst.MessageWarning:
			gerr, err := msg.ParseWarning()
			if err != nil {
				zerolog.Ctx(ctx).Warn().Str("pipeline", p.n).Str("gst_err", strings.ReplaceAll(gerr, "\n", "\\n")).Err(err).Msg("bus watch warning")
				continue
			}

			zerolog.Ctx(ctx).Warn().Str("pipeline", p.n).Str("gst_err", strings.ReplaceAll(gerr, "\n", "\\n")).Msg("bus watch warning")
		}
	}

	return nil
}
