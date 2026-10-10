package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/booleanism/surr/hub/internal/adapter/rabbitmq"
	"github.com/booleanism/surr/hub/internal/protocols/http/middlewares"
	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Node interface {
	AvailableNodes(ctx context.Context, nodes *[]models.Node) (int, error)
}

type Analytics interface {
	GetAnalytics(ctx context.Context, user models.User, node models.Node, filter rabbitmq.AnalyticsFilters, buf *[]byte) (int, error)
}

type AuthHandlerFunc interface {
	RequireAuthHandlerFunc(n http.HandlerFunc) http.HandlerFunc
}

type nodeHandlers struct {
	m    *http.ServeMux
	n    Node
	a    Analytics
	auth AuthHandlerFunc
}

func NewNode(mux *http.ServeMux, n Node, a Analytics, auth AuthHandlerFunc) nodeHandlers {
	h := nodeHandlers{
		m:    mux,
		n:    n,
		a:    a,
		auth: auth,
	}

	mux.HandleFunc("GET /availability", auth.RequireAuthHandlerFunc(http.HandlerFunc(h.getAvailableNodeHandler)))
	mux.HandleFunc("GET /analytics", auth.RequireAuthHandlerFunc(http.HandlerFunc(h.analyticsHandler)))
	return h
}

func (s nodeHandlers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.m.ServeHTTP(w, r)
}

func (n nodeHandlers) analyticsHandler(w http.ResponseWriter, r *http.Request) {
	log := zerolog.Ctx(r.Context())

	buf := pools.BytesPool.Get().(*[]byte)
	defer pools.BytesPoolResetAndPut(buf)

	userID, err := uuid.Parse(r.Header.Get(middlewares.UserIDHeaderKey))
	if err != nil {
		log.Error().Err(err).Msg("parsing user id")
		http.Error(w, "unable to parse user id", http.StatusBadRequest)
		return
	}

	nodeID, err := uuid.Parse(r.URL.Query().Get("node_id"))
	if err != nil {
		log.Error().Err(err).Msg("parsing node id")
		http.Error(w, "unable to parse node id", http.StatusBadRequest)
		return
	}

	user := models.User{
		ID: userID,
	}

	node := models.Node{
		ID: nodeID,
	}

	timeCluster := r.URL.Query().Get("time_cluster")
	if timeCluster == "" {
		timeCluster = "hours"
	}

	filter := rabbitmq.AnalyticsFilters{
		TimeCluster: timeCluster,
	}

	var nbuf int
	nbuf, err = n.a.GetAnalytics(r.Context(), user, node, filter, buf)
	if err != nil {
		log.Error().Str("user_id", userID.String()).Str("node_id", nodeID.String()).Err(err).Msg("get analytics")
		http.Error(w, "unable to fetch analytics", http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	_, _ = w.Write((*buf)[:nbuf])
}

func (n nodeHandlers) getAvailableNodeHandler(w http.ResponseWriter, r *http.Request) {
	log := zerolog.Ctx(r.Context())

	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	_, err := uuid.Parse(r.Header.Get(middlewares.UserIDHeaderKey))
	if err != nil {
		log.Error().Err(err).Msg("parsing user id")
		http.Error(w, "unable to parse user id", http.StatusBadRequest)
		return
	}

	nNodes, err := n.n.AvailableNodes(r.Context(), nodes)
	if err != nil {
		log.Error().Err(err).Msg("available nodes")
		http.Error(w, "could not list available node", http.StatusInternalServerError)
		return
	}

	if nNodes == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(&nodes)
}
