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

func setupPipelines(ctx context.Context, cancel context.CancelCauseFunc, cfg PipelinesConfig) (producer, downstream, recording *core.Pipeline, err error) {
	downstream, err = setupDownstream(cfg.DownstreamPipelineName, cfg.SrtSinkURI, cfg.DownstreamSpecs)
	if err != nil {
		return nil, nil, nil, err
	}

	recSpecs := &cfg.RecordingSpecs
	if cfg.RecordingSpecs.IsZero() {
		recSpecs = nil
	}

	recording, err = setupRecording(cfg.RecordingPipelineName, cfg.RecordingDir, cfg.StreamName, recSpecs)
	if err != nil {
		return nil, nil, nil, err
	}

	producer = core.NewPipeline(cfg.ProducerPipelineName)
	go func() {
		err = producer.WatchBus(ctx)
		if err != nil {
			fmt.Printf("ERRO: %s bus watch: %s\n", producer.GetName(), err.Error())
			cancel(err)
		}
	}()

	go func() {
		err = downstream.WatchBus(ctx)
		if err != nil {
			fmt.Printf("ERRO: %s bus watch: %s\n", downstream.GetName(), err.Error())
			cancel(err)
		}
	}()

	go func() {
		err = recording.WatchBus(ctx)
		if err != nil {
			fmt.Printf("ERRO: %s bus watch: %s\n", recording.GetName(), err.Error())
			cancel(err)
		}
	}()

	return producer, downstream, recording, nil
}

func setupDownstream(pipelineName, srturi string, specs core.StreamSpecs) (*core.Pipeline, error) {
	pipe := core.NewPipeline(pipelineName)

	src := src.NewInterSrc(VidProdName)
	h264enc := coder.NewH264Enc(&specs)
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

	audSrc.GetStaticPad("src").AddProbe(gst.PadProbeTypeBuffer|gst.PadProbeTypeBlock, func(pad gst.Pad, info *gst.PadProbeInfo) (goret gst.PadProbeReturn) {
		caps := pad.GetCurrentCaps()
		if caps == nil {
			return gst.PadProbeOK
		}

		dashsink2.SendEvent(gst.NewEventEOS())
		if ret := dashsink2.AudioSink()(aacenc.GetStaticPad("src")); ret != gst.PadLinkOK {
			return gst.PadProbePass
		}

		dashsink2.SyncStateWithParent()
		return gst.PadProbeRemove
	})

	return pipe, nil
}
