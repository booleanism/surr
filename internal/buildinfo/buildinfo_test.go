package buildinfo

import (
	"runtime/debug"
	"testing"
)

// TestChoosePrefersTheInjectedVersion pins the precedence that makes `make build`
// authoritative: its value is `git describe --tags --always --dirty`, and it is the
// only source that can report a dirty tree.
func TestChoosePrefersTheInjectedVersion(t *testing.T) {
	for _, injected := range []string{"v0.3.0", "v0.3.0-11-g93ea09b", "v0.3.0-11-g93ea09b-dirty"} {
		got := choose(injected, "v0.1.0", []debug.BuildSetting{{Key: "vcs.revision", Value: "aaaaaaaaaaaa"}})
		if got != injected {
			t.Errorf("choose(%q, ...) = %q, want the injected value back", injected, got)
		}
	}
}

// TestChooseFallsBackToTheModuleVersion is the case `go install ...@vX.Y.Z` lands
// in: no -ldflags, so the variable keeps "dev" and the answer comes from the
// module version the toolchain recorded.
func TestChooseFallsBackToTheModuleVersion(t *testing.T) {
	got := choose(Development, "v0.3.0", []debug.BuildSetting{{Key: "vcs.revision", Value: "aaaaaaaaaaaa"}})

	if got != "v0.3.0" {
		t.Fatalf("choose(dev, v0.3.0, ...) = %q, want v0.3.0", got)
	}
}

// TestChooseIgnoresTheDevelModuleVersion is what a bare `go build` in a checkout
// produces: the toolchain records "(devel)" rather than a version, which is not an
// identity and must not be reported as one.
func TestChooseIgnoresTheDevelModuleVersion(t *testing.T) {
	got := choose(Development, develVersion, []debug.BuildSetting{
		{Key: "vcs.revision", Value: "93ea09b1234567890abcdef1234567890abcdef1"},
	})

	if got != "93ea09b" {
		t.Fatalf("choose(dev, (devel), revision) = %q, want the shortened revision", got)
	}
}

// TestChooseTreatsEmptyAsUnset covers the injected-and-module-both-empty path,
// which is a binary built without VCS stamps at all.
func TestChooseTreatsEmptyAsUnset(t *testing.T) {
	if got := choose("", "", nil); got != Development {
		t.Fatalf("choose(\"\", \"\", nil) = %q, want %q", got, Development)
	}
}

func TestResolveNeverReturnsEmpty(t *testing.T) {
	for _, injected := range []string{"", Development, "v0.3.0"} {
		if got := Resolve(injected); got == "" {
			t.Errorf("Resolve(%q) returned an empty version", injected)
		}
	}
}

// TestFromVCSSHortensTheRevision pins the shape a bare `go build` reports. The
// full 40-character hash is noise on every log line the value reaches, and
// `git describe --always` abbreviates for the same reason.
func TestFromVCSSHortensTheRevision(t *testing.T) {
	const full = "93ea09b1234567890abcdef1234567890abcdef1"

	cases := []struct {
		name     string
		settings []debug.BuildSetting
		want     string
	}{
		{
			name:     "clean tree",
			settings: []debug.BuildSetting{{Key: "vcs.revision", Value: full}, {Key: "vcs.modified", Value: "false"}},
			want:     "93ea09b",
		},
		{
			name:     "dirty tree keeps the git describe suffix",
			settings: []debug.BuildSetting{{Key: "vcs.revision", Value: full}, {Key: "vcs.modified", Value: "true"}},
			want:     "93ea09b-dirty",
		},
		{
			name:     "no stamps at all",
			settings: nil,
			want:     Development,
		},
		{
			name:     "a revision shorter than the abbreviation is left alone",
			settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}},
			want:     "abc",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fromVCS(tc.settings); got != tc.want {
				t.Errorf("fromVCS() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestFromVCSIgnoresUnrelatedSettings guards against reading the wrong stamp: the
// build settings carry many keys, and a prefix match or a positional read would
// pick up vcs.time or -buildmode instead.
func TestFromVCSIgnoresUnrelatedSettings(t *testing.T) {
	settings := []debug.BuildSetting{
		{Key: "vcs.time", Value: "2026-10-18T21:43:07Z"},
		{Key: "-buildmode", Value: "exe"},
		{Key: "GOARCH", Value: "amd64"},
	}

	if got := fromVCS(settings); got != Development {
		t.Errorf("fromVCS() = %q, want %q for settings carrying no revision", got, Development)
	}
}
