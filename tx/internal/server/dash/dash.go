package dash

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"path/filepath"

	"github.com/booleanism/surr/tx/internal/streaming"
	"github.com/rs/zerolog"
)

const indexTemplate = `
<!DOCTYPE html>
<html>
  <head>
    <title>Streams</title>
  </head>
  <body>
      <h1 class="title">Available streams:</h1>
      <ul>
        {{ range $name, $url := . }}
        <li><a href="{{ $url }}">{{ $name }}</a></li>
        {{ end }}
      </ul>
  </body>
</html>`

const dashPlayer = `<!DOCTYPE html>
<html>
  <head>
    <title>Stream Player</title>
		<script src="https://cdn.dashjs.org/latest/modern/umd/dash.all.min.js"></script>
  </head>
  <body>
		<video id="video" controls></video>
		<script>
				dashjs.MediaPlayer().create().initialize(
						document.querySelector('#video'),
						'{{ . }}',
						true
				);
		</script>
  </body>
</html>
`

type DashStream interface {
	OpenManifest(s string, w io.Writer) error
	OpenSegment(p string, w io.Writer) error
	AddStream(s string) error
	RemoveStream(s string) error
	GetStreams() *streaming.Stream
}

type DashServer struct {
	t   *template.Template
	d   DashStream
	log zerolog.Logger
}

func NewDashServer(tmpl *template.Template, dash DashStream, log zerolog.Logger) *DashServer {
	return &DashServer{
		t:   tmpl,
		d:   dash,
		log: log,
	}
}

func (d *DashServer) Index(notfound *http.ServeMux) (string, http.HandlerFunc, error) {
	t, err := d.t.New("index").Parse(indexTemplate)
	if err != nil {
		return "", nil, fmt.Errorf("unable to parse index template")
	}

	return "GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		data := make(map[string]string)
		for _, k := range d.d.GetStreams().Names() {
			data[k] = filepath.Join("play", k)
		}

		w.Header().Add("Content-Type", "text/html")

		// A local variable: this closure runs concurrently with every other
		// request, so writing the enclosing function's err here would race.
		execErr := t.Execute(w, data)
		if execErr != nil {
			d.log.Error().Err(execErr).Msg("rendering template")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}, nil
}

func (d *DashServer) PlayStream(manifestFileName string) (string, http.HandlerFunc, error) {
	t, err := d.t.New("player").Parse(dashPlayer)
	if err != nil {
		return "", nil, err
	}

	return "GET /play/{name}", func(w http.ResponseWriter, r *http.Request) {
		d.isStreamExist(r, func(name string) int {
			w.Header().Set("Content-Type", "text/html")
			mpdurl := fmt.Sprintf("/stream/%s/%s", name, manifestFileName)

			execErr := t.Execute(w, mpdurl)
			if execErr != nil {
				d.log.Error().Str("stream", name).Err(execErr).Msg("unable to render play")
				return http.StatusInternalServerError
			}

			return 0
		})(w)
	}, nil
}

func (d *DashServer) StreamManifest(manifestFileName string) (string, http.HandlerFunc) {
	return fmt.Sprintf("GET /stream/{name}/%s", manifestFileName), func(w http.ResponseWriter, r *http.Request) {
		d.isStreamExist(r, func(name string) int {
			w.Header().Set("Content-Type", "application/dash+xml")

			err := d.d.OpenManifest(name, w)
			if err != nil {
				d.log.Error().Str("stream", name).Err(err).Msg("unable to open manifest")
				return http.StatusInternalServerError
			}

			return 0
		})(w)
	}
}

func (d *DashServer) isStreamExist(r *http.Request, onExist func(name string) int) func(w http.ResponseWriter) {
	name := r.PathValue("name")
	if name == "" || !d.d.GetStreams().IsExist(name) {
		return func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusNotFound)
		}
	}

	return func(w http.ResponseWriter) {
		d.log.Debug().Str("method", r.Method).Str("path", r.URL.String()).Msg("dash request")
		ret := onExist(name)
		if ret > 0 {
			w.WriteHeader(ret)
		}
	}
}

func (d *DashServer) StreamSegment() (string, http.HandlerFunc) {
	return "GET /stream/{name}/{segment}", func(w http.ResponseWriter, r *http.Request) {
		d.isStreamExist(r, func(name string) int {
			seg := r.PathValue("segment")

			w.Header().Set("Content-Type", "video/mp4")
			err := d.d.OpenSegment(filepath.Join(name, seg), w)
			if err != nil {
				d.log.Error().Str("stream", name).Str("segment", seg).Err(err).Msg("unable to open segment")
				return http.StatusInternalServerError
			}

			return 0
		})(w)
	}
}

func (d *DashServer) AddStream() (string, http.HandlerFunc) {
	return "POST /stream", func(w http.ResponseWriter, r *http.Request) {
		sn := StreamName{}
		defer r.Body.Close() //nolint:errcheck

		d.decodeStreamBody(r.Body, &sn, func() int {
			err := d.d.AddStream(sn.Name)
			if err != nil {
				d.log.Error().Str("stream", sn.Name).Err(err).Msg("unable to add stream")
				return http.StatusInternalServerError
			}

			return http.StatusOK
		})(w)
	}
}

func (d *DashServer) decodeStreamBody(body io.ReadCloser, jsonable any, f func() int) func(w http.ResponseWriter) {
	err := json.NewDecoder(body).Decode(jsonable)
	if err == nil {
		return func(w http.ResponseWriter) {
			ret := f()
			if ret > 0 {
				w.WriteHeader(ret)
			}
		}
	}

	d.log.Error().Err(err).Msg("unable to decode request")
	return func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (d *DashServer) DeleteStream() (string, http.HandlerFunc) {
	return "DELETE /stream", func(w http.ResponseWriter, r *http.Request) {
		sn := StreamName{}
		defer r.Body.Close() //nolint:errcheck

		d.decodeStreamBody(r.Body, &sn, func() int {
			err := d.d.RemoveStream(sn.Name)
			if err != nil {
				d.log.Error().Str("stream", sn.Name).Err(err).Msg("unable to remove stream")
				return http.StatusInternalServerError
			}

			return http.StatusOK
		})(w)
	}
}
