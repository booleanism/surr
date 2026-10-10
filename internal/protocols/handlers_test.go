package protocols

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
)

// TestErrorResponseIsValidJSON is the regression test for the bug where
// handlers assigned []byte(err.Error()) to the json.RawMessage Result field.
// json.Marshal then failed with "invalid character 's' looking for beginning of
// value", the reply was never published, and the caller timed out instead of
// seeing the error.
func TestErrorResponseIsValidJSON(t *testing.T) {
	const message = "boom \"quoted\"\nsecond line"

	for _, code := range []int{CodeDecodePayload, CodeDomain, CodeMarshalReply} {
		resp := ErrorResponse(code, errors.New(message))

		body, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("code %d: marshal reply: %v", code, err)
		}

		var decoded RouterResponse
		err = json.Unmarshal(body, &decoded)
		if err != nil {
			t.Fatalf("code %d: unmarshal reply %s: %v", code, body, err)
		}

		if decoded.Code != code {
			t.Errorf("code %d: decoded code is %d", code, decoded.Code)
		}

		if got := decoded.ErrorMessage(); got != message {
			t.Errorf("code %d: message is %q, want %q", code, got, message)
		}
	}
}

func TestErrorResponseNilError(t *testing.T) {
	resp := ErrorResponse(CodeDomain, nil)

	if _, err := json.Marshal(resp); err != nil {
		t.Fatalf("marshal reply: %v", err)
	}

	if got := resp.ErrorMessage(); got != "unknown error" {
		t.Errorf("message is %q, want %q", got, "unknown error")
	}
}

func TestResultResponse(t *testing.T) {
	payload := []byte(`{"id":"0f7b1b1e"}`)

	resp := ResultResponse(payload)
	if resp.Code != CodeSuccess {
		t.Fatalf("code is %d, want %d", resp.Code, CodeSuccess)
	}

	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}

	// A success reply omits the zero code, so a caller that only checks for a
	// non-zero code sees exactly the handler payload.
	if string(body) != `{"result":{"id":"0f7b1b1e"}}` {
		t.Errorf("body is %s", body)
	}
}

func TestErrorMessage(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		result json.RawMessage
	}{
		{name: "empty", result: nil, want: ""},
		{name: "null", result: json.RawMessage("null"), want: ""},
		{name: "json string", result: json.RawMessage(`"subscription not found"`), want: "subscription not found"},
		{name: "raw json", result: json.RawMessage(`{"error":"x"}`), want: `{"error":"x"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := RouterResponse{Code: CodeDomain, Result: tt.result}
			if got := resp.ErrorMessage(); got != tt.want {
				t.Errorf("ErrorMessage() is %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetBytes(t *testing.T) {
	raw := []byte{0x01, 0x02, 0x03}
	encoded := base64.StdEncoding.EncodeToString(raw)

	resp := RouterResponse{Code: CodeSuccess, Result: json.RawMessage(`"` + encoded + `"`)}

	got, err := resp.GetBytes()
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}

	if string(got) != string(raw) {
		t.Errorf("GetBytes() is %v, want %v", got, raw)
	}

	// A non-string Result is already the payload.
	resp = RouterResponse{Code: CodeSuccess, Result: json.RawMessage(`[1,2]`)}

	got, err = resp.GetBytes()
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}

	if string(got) != "[1,2]" {
		t.Errorf("GetBytes() is %s, want [1,2]", got)
	}
}
