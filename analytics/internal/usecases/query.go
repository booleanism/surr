package usecases

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

var (
	// ErrInvalidPipeline reports a pipeline this service will not run.
	ErrInvalidPipeline = errors.New("invalid pipeline")

	// ErrUnknownParam reports a parameter that is supplied but not declared, or
	// used by the pipeline but not declared. Both are the same class of mistake:
	// the manifest and the call disagree about the contract.
	ErrUnknownParam = errors.New("unknown query parameter")

	// ErrMissingParam reports a declared parameter the caller did not supply.
	ErrMissingParam = errors.New("missing query parameter")

	// ErrQueryTooLarge is the storage layer's cap failure, re-exported so a
	// caller of this package does not have to reach into models for it. It is an
	// error rather than a truncation because a silently short result is
	// indistinguishable from a small data set, and the caller has no way to ask
	// for the rest.
	ErrQueryTooLarge = models.ErrQueryTooLarge
)

const (
	// maxPipelineStages bounds a single pipeline. The declared pipelines are
	// five to eight stages; the cap is what stops a manifest from turning one
	// query into an unbounded amount of server work.
	maxPipelineStages = 24

	// maxFacetDepth bounds $facet nesting, so a manifest cannot build a
	// pipeline whose stage count the cap cannot see.
	maxFacetDepth = 2

	// maxFleetMembers bounds how many collections one fleet query unions. A
	// union is a stage, and the fleet's size is not something this service
	// controls, so the cap is what stops a large deployment from turning one
	// request into an unbounded aggregation.
	maxFleetMembers = 16

	// maxResultDocuments and maxResultBytes bound what one query may return.
	// Both are enforced by the executor, not by the pipeline, because a pipeline
	// that omits $limit would otherwise return the whole collection.
	maxResultDocuments = 2000
	maxResultBytes     = 1 << 20
)

// allowedStages is the set of aggregation stages a node may declare.
//
// It is an ALLOWLIST, not a blocklist, and that is the whole point: a blocklist
// of $out/$merge/$unionWith loses to a stage that hides one of them, $facet being
// the obvious carrier. With an allowlist an unknown stage is refused by default,
// so a new Mongo release cannot widen what a manifest can do.
//
// $lookup and $graphLookup are absent even though they read rather than write:
// they reach a collection other than the one the manifest declared, which is
// exactly the boundary this service exists to hold.
var allowedStages = map[string]struct{}{
	"$addFields":   {},
	"$bucket":      {},
	"$bucketAuto":  {},
	"$count":       {},
	"$facet":       {},
	"$group":       {},
	"$limit":       {},
	"$match":       {},
	"$project":     {},
	"$replaceRoot": {},
	"$replaceWith": {},
	"$sample":      {},
	"$set":         {},
	"$skip":        {},
	"$sort":        {},
	"$sortByCount": {},
	"$unset":       {},
	"$unwind":      {},
}

// paramTypes is the set of type names a params_schema value may hold.
var paramTypes = map[string]struct{}{
	"string":  {},
	"number":  {},
	"boolean": {},
	"object":  {},
	"array":   {},
}

// Query is the aggregation surface this usecase needs. It is declared here, in
// the consumer, so the Mongo package does not have to name its callers.
type Query interface {
	Aggregate(ctx context.Context, collection string, pipeline []byte, maxDocs, maxBytes int) (json.RawMessage, error)
}

// Declarations is the part of the declaration usecase a query needs. It is
// separate from the storage interface of the same shape so a test can stand in
// for the whole declaration path, not just its last step.
type Declarations interface {
	GetQuery(ctx context.Context, nodeID uuid.UUID, name string, fleet []uuid.UUID) (models.DeclaredQuery, error)
}

type query struct {
	q Query
	d Declarations
}

// NewQuery builds the query usecase.
func NewQuery(q Query, d Declarations) query {
	return query{
		q: q,
		d: d,
	}
}

// Execute runs one declared query and writes the documents to out.
//
// The pipeline is validated twice: structurally when the node declares it, so a
// bad manifest is refused while the author is still looking, and again here,
// because the stored row may have been written by an older build of this
// service whose rules were looser.
func (q query) Execute(ctx context.Context, req models.AnalyticsQueryRequest, out *json.RawMessage) error {
	declared, err := q.d.GetQuery(ctx, req.NodeID, req.QueryName, req.Fleet)
	if err != nil {
		return err
	}

	schema, err := parseParamsSchema(declared.Query.ParamsSchema)
	if err != nil {
		return err
	}

	pipeline, err := substituteParams(declared.Query.Pipeline, schema, req.Params)
	if err != nil {
		return err
	}

	checker := pipelineChecker{sentinel: ErrInvalidPipeline, strict: true}

	stages, err := checker.stages(pipeline)
	if err != nil {
		return err
	}

	if declared.Query.IsFleet() {
		stages, err = unionStages(stages, declared.Collections)
		if err != nil {
			return err
		}
	}

	// The cap is appended rather than merged into the manifest's own $limit: the
	// node's limit may be larger or absent, and appending is the only form that
	// cannot be widened by the pipeline it is meant to bound.
	stages = append(stages, json.RawMessage(fmt.Sprintf(`{"$limit":%d}`, maxResultDocuments+1)))

	capped, err := json.Marshal(stages)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPipeline, err)
	}

	res, err := q.q.Aggregate(ctx, declared.Collection, capped, maxResultDocuments, maxResultBytes)
	if err != nil {
		return err
	}

	*out = res

	return nil
}

// unionStages prepends one $unionWith per member collection beyond the first, so
// the node's own pipeline runs over the whole fleet rather than over one node's
// documents.
//
// This is the ONLY place a $unionWith reaches a pipeline, and it is why the
// allowlist can keep denying it: the collections named here come from this
// service's registry, resolved from the caller's authorized node ids, and never
// from the manifest. A node that sent $unionWith in its own pipeline is still
// refused by the checker, which has already run by the time this is called.
//
// Prepending rather than appending is what makes the semantics right: the
// node's $match, $group and $facet have to see the unioned documents, not the
// result of having already aggregated one collection.
func unionStages(stages []json.RawMessage, collections []string) ([]json.RawMessage, error) {
	if len(collections) <= 1 {
		return stages, nil
	}

	if len(collections) > maxFleetMembers {
		return nil, fmt.Errorf("%w: a fleet of %d nodes is more than the %d this service unions", ErrInvalidPipeline, len(collections), maxFleetMembers)
	}

	// The member list is bounded, but the total is still the node's pipeline plus
	// one stage per member, so the combined count is checked against its own bound
	// rather than against maxPipelineStages -- which is a limit on what a manifest
	// may declare, not on what the service adds.
	if len(stages)+len(collections)-1 > maxPipelineStages+maxFleetMembers {
		return nil, fmt.Errorf("%w: %d stages after the fleet union", ErrInvalidPipeline, len(stages)+len(collections)-1)
	}

	unions := make([]json.RawMessage, 0, len(collections)-1)

	for _, collection := range collections[1:] {
		stage, err := json.Marshal(map[string]any{"$unionWith": map[string]any{"coll": collection}})
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidPipeline, err)
		}

		unions = append(unions, stage)
	}

	return append(unions, stages...), nil
}

// pipelineChecker walks an aggregation pipeline and refuses anything this
// service is not willing to run.
//
// It exists as a type rather than a set of functions because the same walk is
// needed at two moments with different rules: a declaration is checked when it
// arrives, and the pipeline is checked again after parameters have been
// substituted. The difference is strict: at declare time a value may still be a
// placeholder ("${limit}"), so a $limit cannot be required to be a number yet;
// at query time every placeholder has been replaced and the value is checked.
type pipelineChecker struct {
	sentinel error
	strict   bool
}

func (c pipelineChecker) stages(pipeline []byte) ([]json.RawMessage, error) {
	var stages []json.RawMessage

	err := json.Unmarshal(pipeline, &stages)
	if err != nil {
		return nil, fmt.Errorf("%w: the pipeline is not a JSON array: %w", c.sentinel, err)
	}

	if len(stages) == 0 {
		return nil, fmt.Errorf("%w: the pipeline is empty", c.sentinel)
	}

	if len(stages) > maxPipelineStages {
		return nil, fmt.Errorf("%w: %d stages is more than the %d allowed", c.sentinel, len(stages), maxPipelineStages)
	}

	err = c.walk(stages, 0, 0)
	if err != nil {
		return nil, err
	}

	return stages, nil
}

func (c pipelineChecker) walk(stages []json.RawMessage, depth, base int) error {
	for i, stage := range stages {
		err := c.stage(stage, base+i, depth)
		if err != nil {
			return err
		}
	}

	return nil
}

// stage checks one stage and, for $facet, the pipelines nested in it.
func (c pipelineChecker) stage(stage json.RawMessage, index, depth int) error {
	var fields map[string]json.RawMessage

	err := json.Unmarshal(stage, &fields)
	if err != nil {
		return fmt.Errorf("%w: stage %d is not a JSON object: %w", c.sentinel, index, err)
	}

	if len(fields) != 1 {
		// An aggregation stage document holds exactly one operator. Two keys in
		// one stage is not something Mongo rejects outright, but it is always a
		// mistake in a hand-written pipeline and silently ignoring the extra key
		// is how it stays one.
		return fmt.Errorf("%w: stage %d must hold exactly one operator, found %d", c.sentinel, index, len(fields))
	}

	for operator, value := range fields {
		if _, ok := allowedStages[operator]; !ok {
			return fmt.Errorf("%w: stage %d uses %q, which is not an allowed stage", c.sentinel, index, operator)
		}

		if operator == "$facet" {
			err = c.facet(value, index, depth)
			if err != nil {
				return err
			}
		}

		if operator == "$limit" && c.strict {
			err = c.limit(value, index)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// facet descends into a $facet's sub-pipelines. Without this a manifest could
// hide a denied stage one level down, which is why the check is a recursive walk
// rather than a scan of the top-level keys.
func (c pipelineChecker) facet(value json.RawMessage, index, depth int) error {
	if depth >= maxFacetDepth {
		return fmt.Errorf("%w: stage %d nests $facet more than %d deep", c.sentinel, index, maxFacetDepth)
	}

	var pipelines map[string][]json.RawMessage

	err := json.Unmarshal(value, &pipelines)
	if err != nil {
		return fmt.Errorf("%w: stage %d has a $facet that is not an object of pipelines: %w", c.sentinel, index, err)
	}

	for name, stages := range pipelines {
		if len(stages) > maxPipelineStages {
			return fmt.Errorf("%w: $facet %q has %d stages, more than the %d allowed", c.sentinel, name, len(stages), maxPipelineStages)
		}

		err = c.walk(stages, depth+1, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

// limit keeps a declared $limit from being a way around the result cap. Mongo
// rejects a non-numeric $limit anyway; catching it here names the stage.
func (c pipelineChecker) limit(value json.RawMessage, index int) error {
	var limit int64

	err := json.Unmarshal(value, &limit)
	if err != nil {
		return fmt.Errorf("%w: stage %d has a $limit that is not a whole number: %w", c.sentinel, index, err)
	}

	if limit <= 0 {
		return fmt.Errorf("%w: stage %d has a $limit of %d", c.sentinel, index, limit)
	}

	return nil
}

// parseParamsSchema decodes a params_schema into parameter name -> type name.
func parseParamsSchema(raw json.RawMessage) (map[string]string, error) {
	schema := make(map[string]string)

	if len(raw) == 0 {
		return schema, nil
	}

	err := json.Unmarshal(raw, &schema)
	if err != nil {
		return nil, fmt.Errorf("%w: params_schema is not an object of type names: %w", ErrInvalidDeclaration, err)
	}

	for name, kind := range schema {
		if _, ok := paramTypes[kind]; !ok {
			return nil, fmt.Errorf("%w: param %q has type %q, which is not one of string, number, boolean, object or array", ErrInvalidDeclaration, name, kind)
		}
	}

	return schema, nil
}

// substituteParams fills a pipeline template's ${name} placeholders.
//
// The substitution works on the raw bytes rather than on a decoded document: a
// decode and re-encode would reorder the keys of $sort, and that order is part
// of what the query means.
//
// A string that is exactly ${name} is replaced by the JSON encoding of the
// value, so a number stays a number; a placeholder embedded in a longer string
// is replaced by the value's text form, because there is nowhere else for it to
// go.
func substituteParams(pipeline json.RawMessage, schema map[string]string, params map[string]any) (json.RawMessage, error) {
	out := make([]byte, 0, len(pipeline))
	used := make(map[string]struct{}, len(params))

	for i := 0; i < len(pipeline); i++ {
		if pipeline[i] != '"' {
			out = append(out, pipeline[i])
			continue
		}

		end := endOfString(pipeline, i)
		if end < 0 {
			return nil, fmt.Errorf("%w: unterminated string", ErrInvalidPipeline)
		}

		body := string(pipeline[i+1 : end])

		chunk, err := expandString(body, params, used)
		if err != nil {
			return nil, err
		}

		out = append(out, chunk...)

		i = end
	}

	err := checkParams(schema, params, used)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// endOfString returns the index of the quote closing the string that starts at
// start, or -1 when there is none.
func endOfString(body []byte, start int) int {
	for i := start + 1; i < len(body); i++ {
		switch body[i] {
		case '\\':
			i++
		case '"':
			return i
		}
	}

	return -1
}

// expandString renders one JSON string literal, placeholders and all.
func expandString(body string, params map[string]any, used map[string]struct{}) ([]byte, error) {
	name, whole := placeholderName(body)
	if whole {
		value, ok := params[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q is used by the pipeline but was not supplied", ErrMissingParam, name)
		}

		used[name] = struct{}{}

		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("%w: param %q: %w", ErrInvalidPipeline, name, err)
		}

		return encoded, nil
	}

	if !strings.Contains(body, "${") {
		return []byte(`"` + body + `"`), nil
	}

	rendered := body

	for {
		start := strings.Index(rendered, "${")
		if start < 0 {
			break
		}

		stop := strings.Index(rendered[start:], "}")
		if stop < 0 {
			return nil, fmt.Errorf("%w: unterminated ${ in %q", ErrInvalidPipeline, body)
		}

		stop += start

		name = rendered[start+2 : stop]

		value, ok := params[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q is used by the pipeline but was not supplied", ErrMissingParam, name)
		}

		used[name] = struct{}{}

		rendered = rendered[:start] + fmt.Sprint(value) + rendered[stop+1:]
	}

	return []byte(`"` + rendered + `"`), nil
}

// placeholderName reports whether body is exactly one placeholder, and its name.
func placeholderName(body string) (string, bool) {
	if !strings.HasPrefix(body, "${") || !strings.HasSuffix(body, "}") {
		return "", false
	}

	name := body[2 : len(body)-1]
	if strings.Contains(name, "}") || strings.Contains(name, "$") {
		return "", false
	}

	return name, true
}

// checkParams holds both sides of the contract: a parameter the pipeline uses
// must be declared, and a parameter the caller passes must be declared too. An
// undeclared parameter is a typo in one of the two, and silently ignoring it
// would leave the query answering the wrong question.
func checkParams(schema map[string]string, params map[string]any, used map[string]struct{}) error {
	for name := range used {
		if _, ok := schema[name]; !ok {
			return fmt.Errorf("%w: %q is used by the pipeline but not declared in params_schema", ErrUnknownParam, name)
		}
	}

	for name, value := range params {
		kind, ok := schema[name]
		if !ok {
			return fmt.Errorf("%w: %q was supplied but is not declared in params_schema", ErrUnknownParam, name)
		}

		if !matchesType(value, kind) {
			return fmt.Errorf("%w: param %q is %T, which is not a %s", ErrInvalidPipeline, name, value, kind)
		}
	}

	for name := range schema {
		if _, ok := params[name]; !ok {
			return fmt.Errorf("%w: %q is declared in params_schema but was not supplied", ErrMissingParam, name)
		}
	}

	return nil
}

// matchesType reports whether value fits the type name a manifest declared.
func matchesType(value any, kind string) bool {
	switch kind {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		switch value.(type) {
		case float64, int, int64, json.Number:
			return true
		default:
			return false
		}
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}
