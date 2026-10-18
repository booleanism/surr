// Package buildinfo resolves the version string a binary reports for itself.
//
// The Makefile injects one with -ldflags -X main.Version=..., which is what makes
// `make build` produce a binary that names the commit it came from. `go install
// github.com/booleanism/surr/hub/cmd/hub@v0.3.0` cannot: -ldflags is not part of
// a module install, so the variable keeps its default and the binary would report
// "dev" forever.
//
// That default is recoverable. A binary installed from a version suffix records
// the version in its own build info -- `go version -m` shows
// `mod github.com/booleanism/surr v0.3.0` -- so the value is already inside the
// executable, just not where the injected variable looks.
package buildinfo

import (
	"runtime/debug"
	"strings"
)

// Development is the value main.Version carries when nothing set it. It is also
// the answer when the binary genuinely has no identity to report.
const Development = "dev"

// develVersion is what the toolchain records as Main.Version for a build from a
// working copy rather than from a module version.
const develVersion = "(devel)"

// Resolve returns the identity to report for this binary, given whatever was
// injected at link time.
func Resolve(injected string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return choose(injected, "", nil)
	}

	return choose(injected, info.Main.Version, info.Settings)
}

// choose is Resolve's decision without the global state behind it, so the
// precedence can be pinned by a test that does not depend on how the test binary
// itself was built.
func choose(injected, moduleVersion string, settings []debug.BuildSetting) string {
	if injected != "" && injected != Development {
		return injected
	}

	if moduleVersion != "" && moduleVersion != develVersion {
		return moduleVersion
	}

	return fromVCS(settings)
}

// fromVCS reconstructs the `git describe --always --dirty` shape from the
// toolchain's VCS stamps, so a locally built binary and an installed one are
// still distinguishable at a glance.
func fromVCS(settings []debug.BuildSetting) string {
	revision := ""
	modified := false

	for _, s := range settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}

	// Short like `git describe --always` does, because the long form is 40
	// characters of noise in every log line this value reaches.
	if len(revision) > 7 {
		revision = revision[:7]
	}

	if revision == "" {
		return Development
	}

	if modified {
		return strings.Join([]string{revision, "dirty"}, "-")
	}

	return revision
}
