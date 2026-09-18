package streaming

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const registeredFlagFileName = ".registered"

func AvailStream(dash *dash) error {
	err := filepath.WalkDir(dash.r, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if strings.Contains(err.Error(), "permission denied") {
				return nil
			}

			return err
		}

		if strings.Contains(path, ".registered") {
			s := strings.Split(path, "/")
			dash.s.d[s[len(s)-2]] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

type dash struct {
	d StreamDeps
	s *Stream
	r string
	m string
}

func NewDash(deps StreamDeps, recordDir, manifestName string) *dash {
	return &dash{
		d: deps,
		r: recordDir,
		m: manifestName,
		s: &Stream{
			d: make(map[string]struct{}),
		},
	}
}

func (d *dash) RemoveStream(s string) error {
	sp := filepath.Join(d.r, s, registeredFlagFileName)
	_, err := os.Stat(sp)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return nil
	}

	err = os.Remove(sp)
	if err != nil {
		return err
	}

	d.s.Delete(s)
	return nil
}

func (d *dash) AddStream(s string) error {
	sp := filepath.Join(d.r, s, registeredFlagFileName)
	_, err := os.Stat(sp)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		err = os.MkdirAll(filepath.Dir(sp), 0o775)
		if err != nil {
			return err
		}
	}

	f, err := os.Create(sp)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck

	d.s.Set(s, struct{}{})
	return nil
}

func (d *dash) OpenManifest(s string, w io.Writer) error {
	if !d.s.IsExist(s) {
		return fmt.Errorf("stream does not exist")
	}

	return d.openAndWrite(filepath.Join(s, d.m), w)
}

func (d *dash) GetStreams() *Stream {
	return d.s.ref()
}

func (d *dash) OpenSegment(p string, w io.Writer) error {
	ps := strings.Split(p, "/")
	if len(ps) < 1 {
		return fmt.Errorf("invalid path")
	}

	if !d.s.IsExist(ps[0]) {
		return fmt.Errorf("stream does not exist")
	}

	return d.openAndWrite(p, w)
}

func (d *dash) openAndWrite(p string, w io.Writer) error {
	f, err := d.openWithRoot(p)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck

	err = d.writer(f, w)
	if err != nil {
		return err
	}

	return nil
}

func (d *dash) openWithRoot(p string) (WriterToCloser, error) {
	f, err := d.d.OpenInRoot(d.r, p)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (d *dash) writer(f io.WriterTo, w io.Writer) error {
	_, err := f.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}
