package dash

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"path/filepath"

	"github.com/booleanism/surr/tx/internal/streaming"
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
	t *template.Template
	d DashStream
}

func NewDashServer(tmpl *template.Template, dash DashStream) *DashServer {
	return &DashServer{
		t: tmpl,
		d: dash,
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
		s := d.d.GetStreams()
		defer s.Unref()

		for k := range s.Iter() {
			data[k] = filepath.Join("play", k)
		}

		w.Header().Add("Content-Type", "text/html")

		err = t.Execute(w, data)
		if err != nil {
			fmt.Printf("ERRO: rendering template: %s\n", err.Error())
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
			err = t.Execute(w, mpdurl)
			if err != nil {
				fmt.Printf("ERRO: unable to render play: %s\n", err.Error())
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
				fmt.Printf("ERRO: unable to open manifest: %s\n", err.Error())
				return http.StatusInternalServerError
			}

			return 0
		})(w)
	}
}

func (d *DashServer) isStreamExist(r *http.Request, onExist func(name string) int) func(w http.ResponseWriter) {
	name := r.PathValue("name")
	s := d.d.GetStreams()
	defer s.Unref()

	if name == "" || !s.IsExist(name) {
		return func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusNotFound)
		}
	}

	return func(w http.ResponseWriter) {
		fmt.Printf("%s\n", r.URL.String())
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
				fmt.Printf("ERRO: unable to open segment: %s\n", err.Error())
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
				fmt.Printf("ERRO: unable to add stream: %s\n", err.Error())
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

	fmt.Printf("ERRO: unable to decode request: %s\n", err.Error())
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
				fmt.Printf("ERRO: unable to remove stream: %s\n", err.Error())
				return http.StatusInternalServerError
			}

			return http.StatusOK
		})(w)
	}
}
