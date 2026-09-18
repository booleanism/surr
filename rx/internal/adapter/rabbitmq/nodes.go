package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
)

type SiblingGetName interface {
	GetName(c protocols.Command, sn *[]protocols.SiblingName) int
}

type TaskRunner interface {
	Run(ctx context.Context, t pools.TaskFunc) error
}

type nodes struct {
	r protocols.Router
	s SiblingGetName
	t TaskRunner
}

func NewNodes(r protocols.Router, s SiblingGetName, t TaskRunner) nodes {
	return nodes{
		r: r,
		s: s,
		t: t,
	}
}

type NodeSource struct {
	SourceAddr string `json:"source_address"`
}

func (n nodes) StartSink(ctx context.Context, nodes []models.Node, sourceAddr string) error {
	payload, err := json.Marshal(&NodeSource{
		SourceAddr: sourceAddr,
	})
	if err != nil {
		return err
	}

	for _, node := range nodes {
		fmt.Printf("INFO: start node sink: node_id: %s\n", node.ID.String())
		cmd := protocols.Command(fmt.Sprintf("%s.sink.start", node.ID.String()))
		err = n.do(ctx, cmd, payload)
		if err != nil {
			fmt.Printf("ERRO: nodes sink start broadcast: %s\n", err.Error())
		}
	}

	return nil
}

func (n nodes) StopSink(ctx context.Context, nodes []models.Node) error {
	payload, err := json.Marshal(&NodeSource{})
	if err != nil {
		return err
	}

	for _, node := range nodes {
		fmt.Printf("INFO: stop node sink: node_id: %s\n", node.ID.String())
		cmd := protocols.Command(fmt.Sprintf("%s.sink.stop", node.ID.String()))
		err = n.do(ctx, cmd, payload)
		if err != nil {
			fmt.Printf("ERRO: nodes sink stop broadcast: %s\n", err.Error())
		}
	}

	return nil
}

func (n nodes) do(ctx context.Context, c protocols.Command, payload []byte) error {
	sns := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(sns)

	nn := n.s.GetName(c, sns)
	if nn != 1 {
		return fmt.Errorf("expected one sibling with %s command", c)
	}

	sibling := (*sns)[0]
	err := n.t.Run(ctx, func() {
		trackID, err := n.r.Send(ctx, c, payload, sibling)
		if err != nil {
			fmt.Printf("ERRO: send message: %s\n", err.Error())
			return
		}

		resp := protocols.RouterResponse{}
		err = n.r.Recv(ctx, trackID, &resp)
		if err != nil {
			fmt.Printf("ERRO: recv message: %s\n", err.Error())
			return
		}
	})
	if err != nil {
		return err
	}

	return nil
}
