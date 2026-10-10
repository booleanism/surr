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

// maxStreamNameLen bounds a stream name to a single filesystem component.
const maxStreamNameLen = 255

// ErrInvalidStreamName reports a stream name that is not one safe path
// element.
var ErrInvalidStreamName = errors.New("invalid stream name")

// validStreamName rejects anything that is not a single path element.
//
// The name arrives in a request body and is joined onto the recording
// directory before MkdirAll/Create/Remove, so an empty name, "..", a path
// separator or an absolute path would let a caller create or delete files
// outside that directory.
func validStreamName(s string) error {
	switch {
	case s == "" || s == "." || s == "..":
		return fmt.Errorf("%w: %q", ErrInvalidStreamName, s)
	case len(s) > maxStreamNameLen:
		return fmt.Errorf("%w: longer than %d bytes", ErrInvalidStreamName, maxStreamNameLen)
	case s != filepath.Base(s), strings.ContainsAny(s, `/\`):
		return fmt.Errorf("%w: %q is not a single path element", ErrInvalidStreamName, s)
	}

	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %q contains a control character", ErrInvalidStreamName, s)
		}
	}

	return nil
}

type dash struct {
	d StreamDeps
	s *Stream
	r string
	m string
}

// NewDash returns a DASH file store rooted at recordDir.
//
// It also adopts the streams recorded by an earlier process: the .registered
// marker files are the only record of them, so without this scan a restart of
// `tx serve` leaves every previously recorded stream invisible.
func NewDash(deps StreamDeps, recordDir, manifestName string) (*dash, error) {
	d := &dash{
		d: deps,
		r: recordDir,
		m: manifestName,
		s: NewStream(),
	}

	if err := d.loadRegisteredStreams(); err != nil {
		return nil, err
	}

	return d, nil
}

// loadRegisteredStreams registers every stream directory that holds a
// .registered marker. A missing recording directory is not an error: it is
// created on the first AddStream.
func (d *dash) loadRegisteredStreams() error {
	err := filepath.WalkDir(d.r, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}

			if errors.Is(walkErr, fs.ErrPermission) {
				return nil
			}

			return walkErr
		}

		if entry.IsDir() || entry.Name() != registeredFlagFileName {
			return nil
		}

		name := filepath.Base(filepath.Dir(path))
		if validStreamName(name) != nil {
			return nil
		}

		d.s.Set(name)

		return nil
	})
	if err != nil {
		return fmt.Errorf("scanning %s for recorded streams: %w", d.r, err)
	}

	return nil
}

func (d *dash) RemoveStream(s string) error {
	if err := validStreamName(s); err != nil {
		return err
	}

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
	if err := validStreamName(s); err != nil {
		return err
	}

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

	d.s.Set(s)

	return nil
}

func (d *dash) OpenManifest(s string, w io.Writer) error {
	if err := validStreamName(s); err != nil {
		return err
	}

	if !d.s.IsExist(s) {
		return fmt.Errorf("stream does not exist")
	}

	return d.openAndWrite(filepath.Join(s, d.m), w)
}

func (d *dash) GetStreams() *Stream {
	return d.s
}

func (d *dash) OpenSegment(p string, w io.Writer) error {
	name, _, ok := strings.Cut(p, "/")
	if !ok {
		return fmt.Errorf("invalid path")
	}

	if err := validStreamName(name); err != nil {
		return err
	}

	if !d.s.IsExist(name) {
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

	return d.writer(f, w)
}

func (d *dash) openWithRoot(p string) (WriterToCloser, error) {
	return d.d.OpenInRoot(d.r, p)
}

func (d *dash) writer(f io.WriterTo, w io.Writer) error {
	_, err := f.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}
