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
	"github.com/booleanism/surr/tx/internal/server/dash"
	"github.com/booleanism/surr/tx/internal/sink"
	"github.com/booleanism/surr/tx/internal/src"
	"github.com/booleanism/surr/tx/internal/src/rtp"
	"github.com/booleanism/surr/tx/internal/streaming"
	"github.com/go-gst/go-glib/pkg/glib/v2"
	"github.com/go-gst/go-gst/pkg/gst"
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

	beforeExit := []func() error{}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		<-ctx.Done()

		for _, f := range beforeExit {
			err := f()
			if err != nil {
				fmt.Printf("ERRO: before exit: %s\n", err.Error())
			}
		}
	}()

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

	switch cmdLine.SelectedCommand {
	case "serve":
		go func() {
			err = serveCommand(ctx, cmdLine, &beforeExit)
			if err != nil {
				fmt.Printf("ERRO: serve command: %s\n", err.Error())
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
				fmt.Printf("ERRO: run command: %s\n", err.Error())
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
	dashStream := streaming.NewDash(streaming.StreamDeps{
		OpenInRoot: func(s1, s2 string) (streaming.WriterToCloser, error) {
			return os.OpenInRoot(s1, s2)
		},
	}, cmdLine.RecordingDir, ManifestFileName)

	tmpl := template.New("tmpl")
	dash := dash.NewDashServer(tmpl, dashStream)

	var srv *http.Server
	srv, err := setupDashServer(fmt.Sprintf("%s:%d", "127.0.0.1", cmdLine.DashPort), ManifestFileName, dash)
	if err != nil {
		fmt.Printf("ERRO: setup dash server: %s\n", err.Error())
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
			fmt.Printf("INFO: starting DASH server on port %d (dir: %s)\n", cmdLine.DashPort, cmdLine.RecordingDir)
			err = srv.ListenAndServe()
			if err != nil {
				fmt.Printf("ERRO: dash server listen: %s\n", err.Error())
				return err
			}
		}
	}
}

func runCommand(ctx context.Context, cancel context.CancelCauseFunc, cmdLine *CommandLine, beforeExit *[]func() error) error {
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

	rtph264caps, rtph264depay := rtp.NewRTPH264Depay(prodPipe)
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
	rtpmp4acaps, rtpmp4adepay := rtp.NewRTPMP4ADepay(prodPipe)
	cbData[rtpmp4acaps] = rtpmp4adepay.OnPadAdded(aacdec.LinkChain(prodPipe, audlink))

	rtpmp4gcaps, rtpmp4gdepay := rtp.NewRTPMP4GDepay(prodPipe)
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

	fmt.Printf("INFO: running transmitter for %s\n", cmdLine.Run.StreamName)
	pipeSetState(gst.StatePlaying)

	loop.Run()

	return nil
}
