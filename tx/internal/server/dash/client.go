package dash

import (
	"bytes"
	"net/http"
	"net/url"
)

type (
	Command string
	Kind    string
)

const (
	Regist Command = "reg"
	Unreg  Command = "unreg"
)

const (
	JSON  Kind = "json"
	Other Kind = "other"
)

type (
	StreamRequest struct {
		Endpoint string
		CMD      Command
		Kind     Kind
		Data     []byte
	}
	StreamResponse struct {
		StatusCode int
	}
)

type dashClient struct {
	c     *http.Client
	u     *url.URL
	token string
}

// NewDashClient returns a client for the DASH server's control routes. token is
// sent as X-Dash-Token when non-empty; the server requires it for stream
// registration and removal once the bind address is not loopback.
func NewDashClient(c *http.Client, u *url.URL, token string) *dashClient {
	return &dashClient{
		c:     c,
		u:     u,
		token: token,
	}
}

func (c *dashClient) Do(req StreamRequest) (StreamResponse, error) {
	method := c.cmdToHTTPMethod(req.CMD)
	mime := c.kindToMIME(req.Kind)
	body := bytes.NewReader(req.Data)
	endpoint := c.u.JoinPath(req.Endpoint)

	httpReq, err := http.NewRequest(method, endpoint.String(), body)
	if err != nil {
		return StreamResponse{}, err
	}

	httpReq.Header.Set("Content-Type", mime)
	if c.token != "" {
		httpReq.Header.Set("X-Dash-Token", c.token)
	}

	resp, err := c.c.Do(httpReq)
	if err != nil {
		return StreamResponse{}, err
	}
	defer resp.Body.Close()

	return StreamResponse{
		StatusCode: resp.StatusCode,
	}, nil
}

func (c *dashClient) cmdToHTTPMethod(cmd Command) string {
	if cmd == Regist {
		return "POST"
	}

	if cmd == Unreg {
		return "DELETE"
	}

	return "GET"
}

func (c *dashClient) kindToMIME(k Kind) string {
	if k == JSON {
		return "application/json"
	}

	return "text/plain"
}
