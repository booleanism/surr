package usecases

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

// ErrInvalidDeclaration reports a manifest this service will not store: an
// unusable collection name, a query with no name or a name that cannot be
// looked up again, a duplicate name, or a pipeline that is not a JSON array of
// stage documents.
//
// It is deliberately one sentinel rather than a type per defect. The caller is
// a node whose manifest is wrong as a whole, the detail is in the wrapped
// message, and the handler maps the sentinel to one reply code.
var ErrInvalidDeclaration = errors.New("invalid declaration")

// maxCollectionLength, maxQueryNameLength and maxNodeTypeLength bound what a node
// may store. All three are far above any real value; the point is that an
// unbounded string in a primary key is a memory problem waiting for a caller to
// send one.
const (
	maxCollectionLength = 120
	maxQueryNameLength  = 64
	maxNodeTypeLength   = 32
)

// queryNamePattern is the set of names that can be looked up again safely. A
// query name is a primary-key value and a key in the reply map, so it is kept to
// lowercase alphanumerics plus _ . and -, which needs no quoting anywhere it
// travels.
var queryNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)

// nodeTypePattern is the set of node types a fleet may be keyed by. Tighter than
// queryNamePattern because a type is a word, not a path: it names a kind of node
// ("bustle") and never needs a dot or a slash.
var nodeTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Declaration is the storage this service needs.
//
// The interface is declared here, in the consumer, so the sqlite package can
// change shape without the handlers seeing it -- the same arrangement as
// subscriptions' usecases.
type Declaration interface {
	UpsertDeclaration(ctx context.Context, decl models.Declaration) error
	SelectQueryInfos(ctx context.Context, nodeID uuid.UUID, infos *[]models.QueryInfo) (int, error)
	SelectQuery(ctx context.Context, nodeID uuid.UUID, name string) (models.DeclaredQuery, error)
	SelectNodeType(ctx context.Context, nodeID uuid.UUID) (string, error)
	SelectFleetCollections(ctx context.Context, nodeType string, nodeIDs []uuid.UUID, out *[]string) (int, error)
}

type declaration struct {
	d Declaration
}

func NewDeclaration(d Declaration) declaration {
	return declaration{
		d: d,
	}
}

// Declare validates a manifest and replaces whatever that node declared before.
//
// Validation happens here, at declare time, rather than on the query path: a
// manifest is sent once and read many times, so rejecting a bad pipeline once is
// the difference between one error a node author can act on and a permanent
// failure on every read of a query that stored fine.
func (d declaration) Declare(ctx context.Context, decl models.Declaration) error {
	if err := validateDeclaration(decl); err != nil {
		return err
	}

	return d.d.UpsertDeclaration(ctx, decl)
}

// ListQueries returns what the node declared, in name order.
//
// It carries the params schema as well as the name because the caller that runs
// the queries has to know which parameters each one takes: a report endpoint
// supplies a window, and a query that asks for something else has to be reported
// as unanswerable rather than sent a parameter it never declared.
func (d declaration) ListQueries(ctx context.Context, nodeID uuid.UUID, infos *[]models.QueryInfo) (int, error) {
	return d.d.SelectQueryInfos(ctx, nodeID, infos)
}

// GetQuery resolves one declared query and, for a fleet-scoped one, its members.
//
// Membership is resolved here rather than in the executor because it is a
// registry question: the type comes from the node's own declaration and the ids
// come from the caller's authorization, and only the declaration usecase holds
// both. What the executor receives is a finished list of collections, so it never
// has to reach back into the registry mid-query.
func (d declaration) GetQuery(ctx context.Context, nodeID uuid.UUID, name string, fleet []uuid.UUID) (models.DeclaredQuery, error) {
	declared, err := d.d.SelectQuery(ctx, nodeID, name)
	if err != nil {
		return models.DeclaredQuery{}, err
	}

	if !declared.Query.IsFleet() {
		// A node-scoped query ignores the fleet list entirely. Ignoring rather
		// than refusing is deliberate: the caller does not know which scope a
		// name has -- the hub names no query -- so a fleet list that was sent
		// anyway is not a mistake worth failing a report over.
		declared.Scope = models.ScopeNode

		return declared, nil
	}

	// The declaring node's type is what a fleet is keyed by, and it is the same
	// for every node of that type, so the node asking does not have to be the
	// node that declared.
	nodeType, err := d.d.SelectNodeType(ctx, nodeID)
	if err != nil {
		return models.DeclaredQuery{}, err
	}

	collections := make([]string, 0, len(fleet))

	n, err := d.d.SelectFleetCollections(ctx, nodeType, fleet, &collections)
	if err != nil {
		return models.DeclaredQuery{}, err
	}

	if n == 0 {
		// No member collection: the caller owns no node of this type that has
		// declared itself. Answering with the empty shape would read as a fleet
		// that saw nothing, which is the same lie the hub's liveness check
		// refuses for a single node.
		return models.DeclaredQuery{}, fmt.Errorf("%w: no %q node in the caller's fleet has declared itself", models.ErrEmptyFleet, nodeType)
	}

	declared.Collections = collections[:n]
	declared.Collection = collections[0]

	return declared, nil
}

func validateDeclaration(decl models.Declaration) error {
	if decl.NodeID == uuid.Nil {
		return fmt.Errorf("%w: node_id is required", ErrInvalidDeclaration)
	}

	if err := validateCollection(decl.Collection); err != nil {
		return err
	}

	if len(decl.Queries) == 0 {
		return fmt.Errorf("%w: a manifest must declare at least one query", ErrInvalidDeclaration)
	}

	if err := validateNodeType(decl.NodeType, decl.Queries); err != nil {
		return err
	}

	return validateQueries(decl.Queries)
}

// validateNodeType checks the type a fleet query is keyed by.
//
// It is required only when the manifest declares a fleet query, and that is not
// a leniency: a node that declares nothing fleet has no reason to name a type,
// and demanding one would break every node written before fleet declarations
// existed. What it may not do is declare a fleet query without one -- a fleet
// with no type is a fleet that cannot be resolved, so it would be stored and then
// never answerable.
func validateNodeType(nodeType string, queries []models.Query) error {
	fleet := false

	for _, query := range queries {
		if query.IsFleet() {
			fleet = true

			break
		}
	}

	if !fleet {
		return nil
	}

	switch {
	case nodeType == "":
		return fmt.Errorf("%w: a fleet query needs node_type", ErrInvalidDeclaration)
	case len(nodeType) > maxNodeTypeLength:
		return fmt.Errorf("%w: node_type is longer than %d bytes", ErrInvalidDeclaration, maxNodeTypeLength)
	case !nodeTypePattern.MatchString(nodeType):
		return fmt.Errorf("%w: node_type %q must be lowercase alphanumerics plus _ or -", ErrInvalidDeclaration, nodeType)
	}

	return nil
}

func validateCollection(collection string) error {
	switch {
	case collection == "":
		return fmt.Errorf("%w: collection is required", ErrInvalidDeclaration)
	case len(collection) > maxCollectionLength:
		return fmt.Errorf("%w: collection is longer than %d bytes", ErrInvalidDeclaration, maxCollectionLength)
	case strings.HasPrefix(collection, "system."):
		// system.* is Mongo's own bookkeeping: system.views, system.users and
		// friends are not analytics data, and reading them through a declared
		// pipeline would expose the database's internals to any user who can
		// read a node's analytics.
		return fmt.Errorf("%w: collection may not be a system collection", ErrInvalidDeclaration)
	case strings.ContainsRune(collection, '$'), strings.ContainsRune(collection, 0):
		return fmt.Errorf("%w: collection may not contain '$' or a null byte", ErrInvalidDeclaration)
	}

	return nil
}

// validScope accepts an absent scope as well as the two names, because an absent
// scope is what every manifest written before fleet declarations sends and it
// means the same as ScopeNode.
func validScope(scope string) bool {
	switch scope {
	case "", models.ScopeNode, models.ScopeFleet:
		return true
	default:
		return false
	}
}

func validateQueries(queries []models.Query) error {
	seen := make(map[string]struct{}, len(queries))

	for _, query := range queries {
		if err := validateQuery(query); err != nil {
			return err
		}

		if _, duplicate := seen[query.Name]; duplicate {
			return fmt.Errorf("%w: query %q is declared twice", ErrInvalidDeclaration, query.Name)
		}

		seen[query.Name] = struct{}{}
	}

	return nil
}

func validateQuery(query models.Query) error {
	switch {
	case query.Name == "":
		return fmt.Errorf("%w: a query needs a name", ErrInvalidDeclaration)
	case len(query.Name) > maxQueryNameLength:
		return fmt.Errorf("%w: query name %q is longer than %d bytes", ErrInvalidDeclaration, query.Name, maxQueryNameLength)
	case !queryNamePattern.MatchString(query.Name):
		return fmt.Errorf("%w: query name %q must be lowercase alphanumerics plus _ . or -", ErrInvalidDeclaration, query.Name)
	case !validScope(query.Scope):
		return fmt.Errorf("%w: query %q has scope %q, which is neither %q nor %q", ErrInvalidDeclaration, query.Name, query.Scope, models.ScopeNode, models.ScopeFleet)
	}

	if err := validatePipeline(query); err != nil {
		return err
	}

	return validateParamsSchema(query)
}

// validatePipeline requires a JSON array of stage documents.
//
// Only the shape is checked here. Which operators a stage may use is a policy
// the executor enforces, because that is where the pipeline is actually run --
// and it is checked again on the query path, because a manifest stored by an
// earlier build of this service never passed the current policy.
// validatePipeline checks a declared pipeline at declare time.
//
// The operator allowlist is applied here and again at query time. Declaring is
// where the author can still act on the answer, so a pipeline that could never
// run is refused at the door rather than at the first query; re-checking at
// query time is what keeps the guarantee independent of how the manifest got
// into the registry.
//
// The check is deliberately non-strict about values: a pipeline still holds
// placeholders ("${limit}"), so a $limit cannot be required to be a number until
// the parameters have been substituted.
func validatePipeline(query models.Query) error {
	checker := pipelineChecker{sentinel: ErrInvalidDeclaration}

	_, err := checker.stages(query.Pipeline)
	if err != nil {
		// The checker already names the sentinel; only the query is added here, so
		// the message reads once rather than twice.
		return fmt.Errorf("query %q: %w", query.Name, err)
	}

	return nil
}

func validateParamsSchema(query models.Query) error {
	if len(query.ParamsSchema) == 0 {
		return nil
	}

	var schema map[string]json.RawMessage

	if err := json.Unmarshal(query.ParamsSchema, &schema); err != nil {
		return fmt.Errorf("%w: query %q params_schema is not a JSON object: %w", ErrInvalidDeclaration, query.Name, err)
	}

	return nil
}
