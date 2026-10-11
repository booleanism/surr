package coder

import (
	"slices"
	"testing"

	"github.com/go-gst/go-gst/pkg/gst"
)

// The order of the candidate lists is the whole point of the fallback: the
// first installed element wins, so reordering silently changes which decoder or
// encoder a deployment ends up with. On a machine that has all of them -- a
// developer host with deb-multimedia's GStreamer, where fdkaac exists -- the
// difference would not show up, which is exactly why the order is pinned here
// rather than left to be inferred from the code.
func TestAACCandidatesAreInPreferenceOrder(t *testing.T) {
	cases := []struct {
		name       string
		candidates []string
		want       []string
	}{
		{
			name:       "decoder",
			candidates: aacDecCandidates,
			want:       []string{"fdkaacdec", "faad", "avdec_aac"},
		},
		{
			name:       "encoder",
			candidates: aacEncCandidates,
			want:       []string{"fdkaacenc", "voaacenc", "avenc_aac"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !slices.Equal(tc.candidates, tc.want) {
				t.Fatalf("candidates = %v, want %v", tc.candidates, tc.want)
			}
		})
	}
}

// pickElement has to report "nothing here" rather than a nil element with a
// name attached: a caller that checked only the name would go on to link
// through the nil element and crash, which is the bug the fallback exists to
// remove.
func TestPickElementFindsNothingWhenNoCandidateIsInstalled(t *testing.T) {
	gst.Init()

	el, name := pickElement("surr-no-such-element-a", "surr-no-such-element-b")
	if el != nil || name != "" {
		t.Fatalf("pickElement = (%v, %q), want (nil, \"\")", el, name)
	}
}
