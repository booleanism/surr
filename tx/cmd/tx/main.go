package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/booleanism/surr/internal/buildinfo"
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

// Version is the build id reported by --version and logged at startup.
//
// The Makefile injects it with -ldflags -X main.Version=$(VERSION), where VERSION
// is `git describe --tags --always --dirty`. `go install ...@vX.Y.Z` cannot inject
// anything, so it keeps this default; buildinfo.Resolve then recovers the version
// from the binary's own module metadata instead, which is why mainEntry calls it
// before anything reads Version.
var Version = buildinfo.Development

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
	os.Exit(run())
}

// run is main's body, returning the process exit code rather than calling
// os.Exit itself: os.Exit skips deferred calls, and the deferred cancellation
// here is what lets the cleanup hooks run at all.
func run() int {
	// Before anything reads it: --version prints Version and logging.Setup stamps
	// it on every line, and a binary installed from a version suffix has to name
	// that version rather than "dev".
	Version = buildinfo.Resolve(Version)

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
		return 0
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

		return 0

	case "run":
		ctxRun, cancelRun := context.WithCancelCause(ctx)
		cancelWrap := func(err error) {
			cancelRun(err)
			cancel()
		}

		defer cancelWrap(context.DeadlineExceeded)

		// The error travels back here instead of being logged in the goroutine:
		// main is about to exit, and a log line that races the exit is a log
		// line that sometimes does not happen. The buffer means the goroutine
		// never blocks on a reader that has already given up.
		commandDone := make(chan error, 1)

		go func() {
			runErr := runCommand(ctxRun, cancelWrap, cmdLine, cleanups)
			if runErr != nil {
				cancelWrap(runErr)
			}

			commandDone <- runErr
		}()

		<-ctx.Done()
		<-cleanupDone

		// Waiting for the command is what makes the exit code meaningful: a
		// transmitter that could not reach its source has to fail loudly, or a
		// typo in the source URL under `restart: unless-stopped` looks like a
		// service that keeps dying for no stated reason.
		if runErr := <-commandDone; runErr != nil {
			log.Error().Err(runErr).Msg("run command")
			return 1
		}

		return 0

	default:
		return 0
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
	dashSrv := dash.NewDashServer(tmpl, dashStream, *log)

	var srv *http.Server
	srv, err = setupDashServer(fmt.Sprintf("%s:%d", "127.0.0.1", cmdLine.DashPort), ManifestFileName, cmdLine.DashToken, dashSrv, *log)
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

	// Both decoders are built and validated together, before anything is wired:
	// ElementFactoryMake returns nil for a factory this GStreamer installation
	// does not have, and linking through that nil element crashes the process,
	// so a missing plugin has to be a startup error instead.
	prodH264dec := coder.NewH264Dec()
	aacdec := coder.NewAACDec()
	if decErr := coder.CheckDecoders(prodH264dec, aacdec); decErr != nil {
		return decErr
	}

	log.Info().Str("element", aacdec.ElementName()).Msg("aac decoder selected")

	rtph264caps, rtph264depay := rtp.NewRTPH264Depay(prodPipe, *log)
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

	rtpmp4acaps, rtpmp4adepay := rtp.NewRTPMP4ADepay(prodPipe, *log)
	cbData[rtpmp4acaps] = rtpmp4adepay.OnPadAdded(aacdec.LinkChain(prodPipe, audlink))

	rtpmp4gcaps, rtpmp4gdepay := rtp.NewRTPMP4GDepay(prodPipe, *log)
	cbData[rtpmp4gcaps] = rtpmp4gdepay.OnPadAdded(aacdec.LinkChain(prodPipe, audlink))

	// setting up upstream
	rtspSrc := src.NewRTSPSrc(cmdLine.Run.Source, cbData)
	prodPipe.Add(rtspSrc)

	// The loop is stopped by the context rather than by the cleanup hook:
	// g_main_loop_quit is safe from any thread, and this keeps the loop from
	// outliving a cancellation that arrived before the hook was registered.
	go func() {
		<-ctx.Done()
		loop.Quit()
	}()

	cleanups.Register(func() error {
		if os.Getenv("GST_DEBUG_DUMP_DOT_DIR") != "" {
			gst.DebugBinToDotFile(prodPipe, gst.DebugGraphShowAll, ProducerPipelineName)
			gst.DebugBinToDotFile(dsPipe, gst.DebugGraphShowAll, DownstreamPipelineName)

			if recPipe != nil {
				gst.DebugBinToDotFile(recPipe, gst.DebugGraphShowAll, RecordingPipelineName)
			}
		}

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

	// The pipelines are deliberately left in PLAYING rather than set to NULL,
	// and the process exits right here. Going to NULL disposes the RTSP source,
	// which cancels the name lookup it left behind in GIO's threaded resolver;
	// on GLib 2.84 (Debian trixie) that teardown races the resolver's worker
	// thread and kills the process with SIGSEGV inside gthreadedresolver.c.
	// That happens on a clean SIGTERM as much as on an unreachable source, and
	// it is not an ordering problem: it reproduces whether the state change is
	// issued from the loop's thread or from another one, and whether the whole
	// pipeline or only the pipelines downstream of it are brought down. The
	// kernel closes the sockets on exit, so the receiver sees the same thing
	// either way. The cost of leaving this out is that a `--recording` run does
	// not get its trailer written, so an interrupted recording needs remuxing
	// before it plays; that is the better trade against a segfault on every
	// shutdown.
	return runFailure(ctx)
}

// runFailure returns the reason the pipelines came down, or nil when the
// shutdown was asked for rather than caused by a failure. A bus error arrives
// as the cancellation cause (see setupPipelines), so a source that cannot be
// reached comes back out of here and the process exits non-zero.
func runFailure(ctx context.Context) error {
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, context.Canceled) {
		return nil
	}

	return cause
}
