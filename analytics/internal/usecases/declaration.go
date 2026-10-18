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

// maxCollectionLength and maxQueryNameLength bound what a node may store. Both
// are far above any real value; the point is that an unbounded string in a
// primary key is a memory problem waiting for a caller to send one.
const (
	maxCollectionLength = 120
	maxQueryNameLength  = 64
)

// queryNamePattern is the set of names that can be looked up again safely. A
// query name is a primary-key value and a key in the reply map, so it is kept to
// lowercase alphanumerics plus _ . and -, which needs no quoting anywhere it
// travels.
var queryNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)

// Declaration is the storage this service needs.
//
// The interface is declared here, in the consumer, so the sqlite package can
// change shape without the handlers seeing it -- the same arrangement as
// subscriptions' usecases.
type Declaration interface {
	UpsertDeclaration(ctx context.Context, decl models.Declaration) error
	SelectQueryNames(ctx context.Context, nodeID uuid.UUID, names *[]string) (int, error)
	SelectQuery(ctx context.Context, nodeID uuid.UUID, name string) (models.DeclaredQuery, error)
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

func (d declaration) ListQueries(ctx context.Context, nodeID uuid.UUID, names *[]string) (int, error) {
	return d.d.SelectQueryNames(ctx, nodeID, names)
}

func (d declaration) GetQuery(ctx context.Context, nodeID uuid.UUID, name string) (models.DeclaredQuery, error) {
	return d.d.SelectQuery(ctx, nodeID, name)
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

	return validateQueries(decl.Queries)
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
