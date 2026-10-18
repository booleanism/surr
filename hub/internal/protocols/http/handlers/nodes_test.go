package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/booleanism/surr/hub/internal/adapter/rabbitmq"
	"github.com/booleanism/surr/hub/internal/protocols/http/middlewares"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

// The field order is not free: govet's fieldalignment wants the error interface
// packed first.
type fakeAnalytics struct {
	err  error
	body []byte
}

func (f fakeAnalytics) GetAnalytics(
	_ context.Context,
	_ models.User,
	_ models.Node,
	_ rabbitmq.AnalyticsFilters,
	buf *[]byte,
) (int, error) {
	if f.err != nil {
		return 0, f.err
	}

	*buf = append((*buf)[:0], f.body...)

	return len(f.body), nil
}

func getAnalytics(t *testing.T, fake fakeAnalytics) *httptest.ResponseRecorder {
	t.Helper()

	h := NewNode(http.NewServeMux(), nil, fake, fakeAuth{})

	req := httptest.NewRequest(http.MethodGet, "/analytics?node_id="+uuid.New().String(), nil)
	req.Header.Set(middlewares.UserIDHeaderKey, uuid.New().String())
	rec := httptest.NewRecorder()

	h.analyticsHandler(rec, req)

	return rec
}

// TestAnalyticsAnswersConflictForANodeThatIsNotRunning pins the F6 status code.
// The request is well formed and the node is the caller's, so 500 would blame the
// server for a fact about the world; 409 is what tells a client the node has to
// come back before there is anything to report.
func TestAnalyticsAnswersConflictForANodeThatIsNotRunning(t *testing.T) {
	rec := getAnalytics(t, fakeAnalytics{err: models.ErrNodeUnavailable})

	if rec.Code != http.StatusConflict {
		t.Fatalf("status is %d, want %d", rec.Code, http.StatusConflict)
	}
}

// TestAnalyticsKeepsInternalErrorsAsServerErrors is the other half: widening the
// mapping must not swallow every failure into a 409.
func TestAnalyticsKeepsInternalErrorsAsServerErrors(t *testing.T) {
	rec := getAnalytics(t, fakeAnalytics{err: errors.New("declarations unavailable")})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status is %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
