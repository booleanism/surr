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
	c *http.Client
	u *url.URL
}

func NewDashClient(c *http.Client, u *url.URL) *dashClient {
	return &dashClient{
		c: c,
		u: u,
	}
}

func (c *dashClient) Do(req StreamRequest) (StreamResponse, error) {
	method := c.cmdToHTTPMethod(req.CMD)
	mime := c.kindToMIME(req.Kind)
	body := bytes.NewReader(req.Data)
	url := c.u.JoinPath(req.Endpoint)

	httpReq, err := http.NewRequest(method, url.String(), body)
	if err != nil {
		return StreamResponse{}, err
	}

	httpReq.Header.Set("Content-Type", mime)

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
