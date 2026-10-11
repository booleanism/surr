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

	"github.com/booleanism/surr/internal/logging"
	"github.com/booleanism/surr/tx/internal/core"
	"github.com/booleanism/surr/tx/internal/core/coder"
	"github.com/booleanism/surr/tx/internal/server/dash"
	"github.com/booleanism/surr/tx/internal/sink"
	"github.com/booleanism/surr/tx/internal/src"
	"github.com/booleanism/surr/tx/internal/src/rtp"
	"github.com/booleanism/surr/tx/internal/streaming"
	"github.com/go-gst/go-glib/pkg/glib/v2"
	"github.com/go-gst/go-gst/pkg/gst"
	"github.com/rs/zerolog"
)

// Version is the build id reported by --version and logged at startup. The
// Makefile injects it with -ldflags -X main.Version=$(VERSION); see the
// Makefile for how VERSION is derived.
var Version = "dev"

const (
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

	ctx, log := logging.Setup(ctx, "tx", "", Version, cmdLine.LogLevel, cmdLine.LogFormat)

	cleanups := &logging.Cleanups{}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		<-ctx.Done()

		for _, cleanupErr := range cleanups.Run() {
			log.Error().Err(cleanupErr).Msg("before exit")
		}
	}()

	switch cmdLine.SelectedCommand {
	case "serve":
		go func() {
			if serveErr := serveCommand(ctx, cmdLine, cleanups); serveErr != nil {
				log.Error().Err(serveErr).Msg("serve command")
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
			if runErr := runCommand(ctxRun, cancelWrap, cmdLine, cleanups); runErr != nil {
				log.Error().Err(runErr).Msg("run command")
				cancelWrap(runErr)
				return
			}
		}()

		<-ctx.Done()
		<-cleanupDone

	default:
	}
}

func serveCommand(ctx context.Context, cmdLine *CommandLine, cleanups *logging.Cleanups) error {
	log := zerolog.Ctx(ctx)

	dashStream, err := streaming.NewDash(streaming.StreamDeps{
		OpenInRoot: func(s1, s2 string) (streaming.WriterToCloser, error) {
			return os.OpenInRoot(s1, s2)
		},
	}, cmdLine.RecordingDir, ManifestFileName)
	if err != nil {
		log.Error().Err(err).Msg("open recording dir")
		return err
	}

	tmpl := template.New("tmpl")
	dash := dash.NewDashServer(tmpl, dashStream, *log)

	var srv *http.Server
	srv, err = setupDashServer(fmt.Sprintf("%s:%d", "127.0.0.1", cmdLine.DashPort), ManifestFileName, cmdLine.DashToken, dash, *log)
	if err != nil {
		log.Error().Err(err).Msg("setup dash server")
		return err
	}

	cleanups.Register(func() error {
		if closeErr := srv.Close(); closeErr != nil {
			return fmt.Errorf("closing dash server: %w", closeErr)
		}

		return nil
	})

	for {
		select {
		case <-ctx.Done():
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			return nil
		default:
			log.Info().Int("port", int(cmdLine.DashPort)).Str("dir", cmdLine.RecordingDir).Msg("starting DASH server")
			if listenErr := srv.ListenAndServe(); listenErr != nil {
				log.Error().Err(listenErr).Msg("dash server listen")
				return listenErr
			}
		}
	}
}

// registDashStream tells the DASH server about a new recording and returns a
// function that removes it again, or nil when registration was not possible.
//
// It is best effort: the DASH server is a separate process that may not be
// running yet, and tx run is expected to relay and record without one, so a
// failure is logged and the recording still proceeds.
func registDashStream(ctx context.Context, cmdLine *CommandLine, streamName dash.StreamName) func() {
	log := zerolog.Ctx(ctx)

	dashURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", cmdLine.DashPort))
	if err != nil {
		log.Error().Err(err).Msg("parse dash url")
		return nil
	}

	c := &http.Client{Timeout: GeneralTimeout}
	dashManager := dash.NewStreamManager(dash.NewDashClient(c, dashURL, cmdLine.DashToken).Do)

	if regErr := dashManager.RegistStream(streamName); regErr != nil {
		log.Warn().Err(regErr).Str("stream", streamName.Name).
			Msg("unable to register stream with the dash server; recording will still be written but will not appear in the dash index")
		return nil
	}

	return func() {
		if unregErr := dashManager.UnregStream(streamName); unregErr != nil {
			log.Warn().Err(unregErr).Str("stream", streamName.Name).Msg("unable to unregister stream")
		}
	}
}

func runCommand(ctx context.Context, cancel context.CancelCauseFunc, cmdLine *CommandLine, cleanups *logging.Cleanups) error {
	log := zerolog.Ctx(ctx)

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
			if p != nil {
				p.SetState(st)
			}
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

	pipes = append(pipes, prodPipe, dsPipe)
	if recPipe != nil {
		pipes = append(pipes, recPipe)
	}

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

	cleanups.Register(func() error {
		if os.Getenv("GST_DEBUG_DUMP_DOT_DIR") != "" {
			gst.DebugBinToDotFile(prodPipe, gst.DebugGraphShowAll, ProducerPipelineName)
			gst.DebugBinToDotFile(dsPipe, gst.DebugGraphShowAll, DownstreamPipelineName)

			if recPipe != nil {
				gst.DebugBinToDotFile(recPipe, gst.DebugGraphShowAll, RecordingPipelineName)
			}
		}

		pipeSetState(gst.StateNull)
		loop.Quit()
		return nil
	})

	if recPipe != nil {
		if unreg := registDashStream(ctx, cmdLine, streamName); unreg != nil {
			cleanups.Register(func() error {
				unreg()
				return nil
			})
		}
	}

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
