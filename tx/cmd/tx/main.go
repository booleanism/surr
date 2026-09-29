package main

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/booleanism/surr/tx/internal/core"
	"github.com/booleanism/surr/tx/internal/core/coder"
	"github.com/booleanism/surr/tx/internal/logging"
	"github.com/booleanism/surr/tx/internal/server/dash"
	"github.com/booleanism/surr/tx/internal/sink"
	"github.com/booleanism/surr/tx/internal/src"
	"github.com/booleanism/surr/tx/internal/src/rtp"
	"github.com/booleanism/surr/tx/internal/streaming"
	"github.com/go-gst/go-glib/pkg/glib/v2"
	"github.com/go-gst/go-gst/pkg/gst"
	"github.com/rs/zerolog"
)

const (
	Version          = "1.0.0"
	VidProdName      = "vid"
	AudProdName      = "aud"
	ManifestFileName = "manifest.mpd"

	GeneralTimeout = 5 * time.Second

	DownstreamPipelineName = "downstream"
	ProducerPipelineName   = "producer"
	RecordingPipelineName  = "recording"
)

var DefaultStreamSpec = core.StreamSpecs{
	Width:   1280,
	Height:  720,
	Bitrate: 300,
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cmdLine := &CommandLine{
		Version: Version,
		Run: RunCmdLine{
			DownstreamSpecs: DefaultStreamSpec,
		},
	}
	err := cmdLine.parseCLI(ctx, os.Args)
	if err != nil {
		return
	}

	ctx, log := logging.Setup(ctx, "tx", cmdLine.LogLevel, cmdLine.LogFormat)

	beforeExit := []func() error{}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		<-ctx.Done()

		for _, f := range beforeExit {
			err = f()
			if err != nil {
				log.Error().Err(err).Msg("before exit")
			}
		}
	}()

	switch cmdLine.SelectedCommand {
	case "serve":
		go func() {
			err = serveCommand(ctx, cmdLine, &beforeExit)
			if err != nil {
				log.Error().Err(err).Msg("serve command")
				return
			}
		}()

		<-ctx.Done()
		<-cleanupDone

	case "run":
		ctxRun, cancelRun := context.WithCancelCause(ctx)
		cancelWrap := func(err error) {
			cancelRun(err)
			cancel()
		}

		defer cancelWrap(context.DeadlineExceeded)

		go func() {
			err = runCommand(ctxRun, cancelWrap, cmdLine, &beforeExit)
			if err != nil {
				log.Error().Err(err).Msg("run command")
				cancelWrap(err)
				return
			}
		}()

		<-ctx.Done()
		<-cleanupDone

	default:
	}
}

func serveCommand(ctx context.Context, cmdLine *CommandLine, beforeExit *[]func() error) error {
	log := zerolog.Ctx(ctx)

	dashStream := streaming.NewDash(streaming.StreamDeps{
		OpenInRoot: func(s1, s2 string) (streaming.WriterToCloser, error) {
			return os.OpenInRoot(s1, s2)
		},
	}, cmdLine.RecordingDir, ManifestFileName)

	tmpl := template.New("tmpl")
	dash := dash.NewDashServer(tmpl, dashStream, *log)

	var srv *http.Server
	srv, err := setupDashServer(fmt.Sprintf("%s:%d", "127.0.0.1", cmdLine.DashPort), ManifestFileName, dash, *log)
	if err != nil {
		log.Error().Err(err).Msg("setup dash server")
		return err
	}

	(*beforeExit) = append((*beforeExit), func() error {
		err = srv.Close()
		if err != nil {
			return fmt.Errorf("closing dash server: %w", err)
		}

		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			log.Info().Int("port", int(cmdLine.DashPort)).Str("dir", cmdLine.RecordingDir).Msg("starting DASH server")
			err = srv.ListenAndServe()
			if err != nil {
				log.Error().Err(err).Msg("dash server listen")
				return err
			}
		}
	}
}

func runCommand(ctx context.Context, cancel context.CancelCauseFunc, cmdLine *CommandLine, beforeExit *[]func() error) error {
	log := zerolog.Ctx(ctx)

	dashURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", cmdLine.DashPort))
	streamName := dash.StreamName{
		Name: cmdLine.Run.StreamName,
	}

	gst.Init()

	gCtx := glib.NewMainContext()
	loop := glib.NewMainLoop(gCtx, false)
	cbData := make(src.PadAddedCallback)

	pipes := []*core.Pipeline{}
	pipeSetState := func(st gst.State) {
		for _, p := range pipes {
			p.SetState(st)
		}
	}

	prodPipe, dsPipe, recPipe, err := setupPipelines(ctx, cancel, PipelinesConfig{
		ProducerPipelineName:   ProducerPipelineName,
		DownstreamPipelineName: DownstreamPipelineName,
		RecordingPipelineName:  RecordingPipelineName,
		RecordingDir:           cmdLine.RecordingDir,
		StreamName:             cmdLine.Run.StreamName,
		SrtSinkURI:             cmdLine.Run.Sink,
		DownstreamSpecs:        cmdLine.Run.DownstreamSpecs,
		RecordingSpecs:         cmdLine.Run.RecordingSpecs,
	})
	if err != nil {
		return err
	}

	pipes = append(pipes, prodPipe, dsPipe, recPipe)

	// video related stuff
	vidSink := sink.NewInterSink(VidProdName)
	prodPipe.Add(vidSink)

	rtph264caps, rtph264depay := rtp.NewRTPH264Depay(prodPipe, *log)
	prodH264dec := coder.NewH264Dec()
	cbData[rtph264caps] = rtph264depay.OnPadAdded(prodH264dec.LinkChain(prodPipe, func(srcpad gst.Pad) gst.PadLinkReturn {
		return srcpad.Link(vidSink.GetStaticPad("sink"))
	}))

	// audio relared struff
	audSink := sink.NewInterSink(AudProdName)
	audlink := func(srcpad gst.Pad) gst.PadLinkReturn {
		prodPipe.Add(audSink)
		audSink.SyncStateWithParent()
		return srcpad.Link(audSink.GetStaticPad("sink"))
	}

	aacdec := coder.NewAACDec()
	rtpmp4acaps, rtpmp4adepay := rtp.NewRTPMP4ADepay(prodPipe, *log)
	cbData[rtpmp4acaps] = rtpmp4adepay.OnPadAdded(aacdec.LinkChain(prodPipe, audlink))

	rtpmp4gcaps, rtpmp4gdepay := rtp.NewRTPMP4GDepay(prodPipe, *log)
	cbData[rtpmp4gcaps] = rtpmp4gdepay.OnPadAdded(aacdec.LinkChain(prodPipe, audlink))

	// setting up upstream
	rtspSrc := src.NewRTSPSrc(cmdLine.Run.Source, cbData)
	prodPipe.Add(rtspSrc)

	(*beforeExit) = append((*beforeExit), func() error {
		if os.Getenv("GST_DEBUG_DUMP_DOT_DIR") != "" {
			gst.DebugBinToDotFile(prodPipe, gst.DebugGraphShowAll, ProducerPipelineName)
			gst.DebugBinToDotFile(dsPipe, gst.DebugGraphShowAll, DownstreamPipelineName)
			gst.DebugBinToDotFile(recPipe, gst.DebugGraphShowAll, RecordingPipelineName)
		}

		pipeSetState(gst.StateNull)
		loop.Quit()
		return nil
	})

	c := &http.Client{
		Timeout: GeneralTimeout,
	}

	dashClient := dash.NewDashClient(c, dashURL)
	dashManager := dash.NewStreamManager(dashClient.Do)
	err = dashManager.RegistStream(streamName)
	if err != nil {
		return fmt.Errorf("unable to regist stream: %w", err)
	}

	(*beforeExit) = append((*beforeExit), func() error {
		err = dashManager.UnregStream(streamName)
		if err != nil {
			return fmt.Errorf("unable to unregister stream: %w", err)
		}

		return nil
	})

	log.Info().
		Str("stream", cmdLine.Run.StreamName).
		Str("src", logging.RedactURL(cmdLine.Run.Source)).
		Str("sink", logging.RedactURL(cmdLine.Run.Sink)).
		Uint64("downstream_width", cmdLine.Run.DownstreamSpecs.Width).
		Uint64("downstream_height", cmdLine.Run.DownstreamSpecs.Height).
		Uint32("downstream_bitrate", cmdLine.Run.DownstreamSpecs.Bitrate).
		Uint64("recording_width", cmdLine.Run.RecordingSpecs.Width).
		Uint64("recording_height", cmdLine.Run.RecordingSpecs.Height).
		Uint32("recording_bitrate", cmdLine.Run.RecordingSpecs.Bitrate).
		Msg("running transmitter")

	pipeSetState(gst.StatePlaying)

	loop.Run()

	return nil
}
