package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/booleanism/surr/tx/internal/core"
	"github.com/booleanism/surr/tx/internal/core/coder"
	"github.com/booleanism/surr/tx/internal/core/muxer"
	"github.com/booleanism/surr/tx/internal/sink"
	"github.com/booleanism/surr/tx/internal/src"
	"github.com/go-gst/go-gst/pkg/gst"
	"github.com/rs/zerolog"
)

type PipelinesConfig struct {
	ProducerPipelineName   string
	DownstreamPipelineName string
	RecordingPipelineName  string
	SrtSinkURI             string
	RecordingDir           string
	StreamName             string
	DownstreamSpecs        core.StreamSpecs
	RecordingSpecs         core.StreamSpecs
}

// setupPipelines builds the producer, downstream and (optionally) recording
// pipelines and starts a bus watch on each.
//
// recording is nil when --recording is 0,0,0: recording is opt-in, and the
// pipeline used to be built regardless, which created the stream directory and
// registered the stream even though nothing was being recorded.
func setupPipelines(ctx context.Context, cancel context.CancelCauseFunc, cfg PipelinesConfig) (producer, downstream, recording *core.Pipeline, err error) {
	downstream, err = setupDownstream(cfg.DownstreamPipelineName, cfg.SrtSinkURI, cfg.DownstreamSpecs)
	if err != nil {
		return nil, nil, nil, err
	}

	if !cfg.RecordingSpecs.IsZero() {
		recording, err = setupRecording(cfg.RecordingPipelineName, cfg.RecordingDir, cfg.StreamName, &cfg.RecordingSpecs)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	producer = core.NewPipeline(cfg.ProducerPipelineName)

	watched := []*core.Pipeline{producer, downstream}
	if recording != nil {
		watched = append(watched, recording)
	}

	for _, pipe := range watched {
		// A local watchErr, not the enclosing named return: these goroutines
		// run concurrently, so assigning a shared err is a data race and one
		// pipeline's failure could overwrite another's before it is read.
		go func() {
			if watchErr := pipe.WatchBus(ctx); watchErr != nil {
				zerolog.Ctx(ctx).Error().Str("pipeline", pipe.GetName()).Err(watchErr).Msg("bus watch")
				cancel(watchErr)
			}
		}()
	}

	return producer, downstream, recording, nil
}

func setupDownstream(pipelineName, srturi string, specs core.StreamSpecs) (*core.Pipeline, error) {
	pipe := core.NewPipeline(pipelineName)

	src := src.NewInterSrc(VidProdName)
	h264enc := coder.NewH264Enc(&specs)
	if !h264enc.Valid() {
		return nil, fmt.Errorf("h264 encoder elements unavailable: install openh264enc, or build with -tags va for vah264enc")
	}

	mpegtsmux := muxer.NewMPEGTSMux()
	sink := sink.NewSRTSink(srturi)

	if !pipe.AddMany(src, h264enc, mpegtsmux, sink) {
		return nil, fmt.Errorf("unable to add downstream elements")
	}

	if !gst.LinkMany(src, h264enc) || !gst.LinkMany(mpegtsmux, sink) {
		return nil, fmt.Errorf("unable to link downstream elements")
	}

	if ret := mpegtsmux.VideoSink()(h264enc.GetStaticPad("src")); ret != gst.PadLinkOK {
		return nil, fmt.Errorf("unable to link video downstream: %s", ret.String())
	}

	return pipe, nil
}

func setupRecording(pipelineName, p, streamName string, recSpecs *core.StreamSpecs) (*core.Pipeline, error) {
	pipe := core.NewPipeline(pipelineName)
	vidSrc := src.NewInterSrc(VidProdName)
	h264enc := coder.NewH264Enc(recSpecs)
	if !h264enc.Valid() {
		return nil, fmt.Errorf("h264 encoder elements unavailable: install openh264enc, or build with -tags va for vah264enc")
	}

	audSrc := src.NewInterSrc(AudProdName)
	aacenc := coder.NewAACEnc()

	dashsink2 := sink.NewDashSink2(map[string]any{
		"mpd-root-path":   filepath.Join(p, streamName),
		"mpd-filename":    ManifestFileName,
		"target-duration": int32(2000),
		"dynamic":         true,
	})

	if !pipe.AddMany(vidSrc, h264enc, audSrc, aacenc, dashsink2) {
		return nil, fmt.Errorf("unable to add recording elements")
	}

	if !vidSrc.Link(h264enc) {
		return nil, fmt.Errorf("unable to link video recording elements")
	}
	if ret := dashsink2.VideoSink()(h264enc.GetStaticPad("src")); ret != gst.PadLinkOK {
		return nil, fmt.Errorf("unable to link video recording: %s", ret)
	}

	if !audSrc.Link(aacenc) {
		return nil, fmt.Errorf("unable to link audio recording elements")
	}

	// Link the muxer's audio pad lazily, on the first buffer that carries real
	// caps, then remove the probe. The probe used to send an EOS to the muxer
	// on every buffer it saw, which ended the audio branch of the recording;
	// it also blocked the streaming thread (PadProbeTypeBlock) while doing so.
	audSrc.GetStaticPad("src").AddProbe(gst.PadProbeTypeBuffer, func(pad gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
		if pad.GetCurrentCaps() == nil {
			return gst.PadProbeOK
		}

		if ret := dashsink2.AudioSink()(aacenc.GetStaticPad("src")); ret != gst.PadLinkOK {
			// The muxer is not ready for the pad yet; try again on the next
			// buffer rather than giving up or emitting EOS.
			return gst.PadProbeOK
		}

		dashsink2.SyncStateWithParent()

		return gst.PadProbeRemove
	})

	return pipe, nil
}
