package models

import (
	"fmt"
	"net/url"
)

type SRTInfo struct {
	Options map[string]string
	Host    string
	Port    uint16
}

func (s SRTInfo) String() string {
	url := &url.URL{
		Scheme: "srt",
		Host:   fmt.Sprintf("%s:%d", s.Host, s.Port),
	}

	q := url.Query()
	for k, v := range s.Options {
		q.Add(k, v)
	}

	url.RawQuery = q.Encode()
	return url.String()
}
