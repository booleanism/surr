package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
)

type Node interface {
	AvailableNodes(ctx context.Context, nodes *[]models.Node) (int, error)
}

type nodeHandlers struct {
	m *http.ServeMux
	n Node
}

func NewNode(mux *http.ServeMux, n Node) nodeHandlers {
	h := nodeHandlers{
		m: mux,
		n: n,
	}

	mux.HandleFunc("GET /availability", h.getAvailableNodeHandler)
	return h
}

func (s nodeHandlers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.m.ServeHTTP(w, r)
}

func (n *nodeHandlers) getAvailableNodeHandler(w http.ResponseWriter, r *http.Request) {
	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	// _, err := uuid.Parse(r.Header.Get(middlewares.UserIDHeaderKey))
	// if err != nil {
	//	fmt.Printf("ERRO: parsing user id: %s\n", err.Error())
	// 	http.Error(w, "unable to parse user id", http.StatusBadRequest)
	// 	return
	// }

	nNodes, err := n.n.AvailableNodes(r.Context(), nodes)
	if err != nil {
		fmt.Printf("ERRO: available nodes: %s\n", err.Error())
		http.Error(w, "could not list available node", http.StatusInternalServerError)
		return
	}

	if nNodes == 0 {
		http.Error(w, "zero nodes", http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(&nodes)
}
