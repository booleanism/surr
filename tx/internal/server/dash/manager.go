package dash

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type StreamName struct {
	Name string `json:"name"`
}

type streamManager struct {
	do func(req StreamRequest) (StreamResponse, error)
}

func NewStreamManager(do func(StreamRequest) (StreamResponse, error)) *streamManager {
	return &streamManager{
		do: do,
	}
}

func (s *streamManager) RegistStream(stream StreamName) error {
	reqBody, err := json.Marshal(stream)
	if err != nil {
		return err
	}

	req := StreamRequest{
		Endpoint: "stream",
		CMD:      Regist,
		Data:     reqBody,
		Kind:     JSON,
	}

	resp, err := s.do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unable to regist stream: %s", stream.Name)
	}

	return nil
}

func (s *streamManager) UnregStream(stream StreamName) error {
	reqBody, err := json.Marshal(stream)
	if err != nil {
		return err
	}

	req := StreamRequest{
		Endpoint: "stream",
		Kind:     JSON,
		CMD:      Unreg,
		Data:     reqBody,
	}
	resp, err := s.do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unable to unreg stream: %s", stream.Name)
	}

	return nil
}
