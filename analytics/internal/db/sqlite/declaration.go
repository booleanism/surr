package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

// ErrBufferFull reports that a caller-supplied result buffer could not hold
// every row. A truncated read must not be indistinguishable from a small result
// set, which is what returning a short count with a nil error would do.
var ErrBufferFull = errors.New("result buffer full")

// ErrNothingToDeclare reports a declaration carrying no queries. It is refused
// here as well as in the usecase because an empty manifest would delete the
// node's previous one and leave nothing in its place.
var ErrNothingToDeclare = errors.New("nothing to declare")

// ErrDeclarationNotFound reports a lookup for a query the node never declared.
// It is the shared sentinel re-exported, because the handler that answers a
// missing declaration has to recognise it without importing this package.
var ErrDeclarationNotFound = models.ErrDeclarationNotFound

const (
	insertDeclarationStmt = `
		INSERT INTO declarations (node_id, name, collection, description, params_schema, pipeline, updated_at)
		VALUES %s
	`
	deleteDeclarationStmt = `DELETE FROM declarations WHERE node_id = ?`
	selectQueryNamesStmt  = `SELECT name FROM declarations WHERE node_id = ? ORDER BY name`
	selectQueryInfosStmt  = `SELECT name, description, params_schema FROM declarations WHERE node_id = ? ORDER BY name`
	selectQueryStmt       = `SELECT collection, description, params_schema, pipeline FROM declarations WHERE node_id = ? AND name = ?`
)

type declaration struct {
	*db.Transaction
}

func NewDeclaration(txFactory db.TXFactoryFunc) declaration {
	return declaration{
		Transaction: txFactory(),
	}
}

// UpsertDeclaration replaces the whole manifest for one node.
//
// Delete-then-insert rather than INSERT OR REPLACE: a manifest is a set, and a
// re-declare that drops a query has to remove it. Upserting only the queries
// that were sent would keep answering a query the node no longer offers, and
// the caller has no way to tell the two apart.
func (d declaration) UpsertDeclaration(ctx context.Context, decl models.Declaration) error {
	if len(decl.Queries) == 0 {
		return fmt.Errorf("%w: node %s", ErrNothingToDeclare, decl.NodeID)
	}

	cleanup, err := d.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err = d.DB.ExecContext(ctx, deleteDeclarationStmt, decl.NodeID[:]); err != nil {
		_ = d.Rollback()
		return err
	}

	valueStrings := make([]string, 0, len(decl.Queries))
	valueArgs := make([]any, 0, len(decl.Queries)*7)
	now := time.Now().Unix()

	for _, query := range decl.Queries {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs,
			decl.NodeID[:],
			query.Name,
			decl.Collection,
			query.Description,
			rawOrFallback(query.ParamsSchema, "{}"),
			rawOrFallback(query.Pipeline, "[]"),
			now,
		)
	}

	stmt := fmt.Sprintf(insertDeclarationStmt, strings.Join(valueStrings, ", "))

	if _, err = d.DB.ExecContext(ctx, stmt, valueArgs...); err != nil {
		_ = d.Rollback()
		return err
	}

	return d.Commit()
}

// SelectQueryNames returns the declared query names for one node, sorted.
//
// Sorted because the reply is read by a caller that may show the list to a
// human, and because SQLite's row order is not otherwise promised.
func (d declaration) SelectQueryNames(ctx context.Context, nodeID uuid.UUID, names *[]string) (int, error) {
	rws, err := d.DB.QueryContext(ctx, selectQueryNamesStmt, nodeID[:])
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	i := 0
	for rws.Next() {
		if i >= cap(*names) {
			return i, fmt.Errorf("%w: stopped after %d rows", ErrBufferFull, i)
		}

		*names = append(*names, "")

		if err = rws.Scan(&(*names)[i]); err != nil {
			return i, err
		}

		i++
	}

	return i, rws.Err()
}

// SelectQueryInfos returns every declared query for one node, sorted by name,
// with the metadata a caller needs to run each of them and nothing else.
//
// Sorted for the same reason as SelectQueryNames: the reply drives what a human
// is shown, and SQLite's row order is not otherwise promised. The order is also
// what makes a report stable across requests.
func (d declaration) SelectQueryInfos(ctx context.Context, nodeID uuid.UUID, infos *[]models.QueryInfo) (int, error) {
	rws, err := d.DB.QueryContext(ctx, selectQueryInfosStmt, nodeID[:])
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	i := 0
	for rws.Next() {
		if i >= cap(*infos) {
			return i, fmt.Errorf("%w: stopped after %d rows", ErrBufferFull, i)
		}

		*infos = append(*infos, models.QueryInfo{})

		var paramsSchema string

		if err = rws.Scan(&(*infos)[i].Name, &(*infos)[i].Description, &paramsSchema); err != nil {
			return i, err
		}

		(*infos)[i].ParamsSchema = json.RawMessage(paramsSchema)
		i++
	}

	return i, rws.Err()
}

// SelectQuery returns one declared query with the collection it reads.
func (d declaration) SelectQuery(ctx context.Context, nodeID uuid.UUID, name string) (models.DeclaredQuery, error) {
	var (
		collection   string
		description  string
		paramsSchema string
		pipeline     string
	)

	rw := d.DB.QueryRowContext(ctx, selectQueryStmt, nodeID[:], name)

	err := rw.Scan(&collection, &description, &paramsSchema, &pipeline)
	if errors.Is(err, sql.ErrNoRows) {
		return models.DeclaredQuery{}, fmt.Errorf("%w: node %s query %s", ErrDeclarationNotFound, nodeID, name)
	}

	if err != nil {
		return models.DeclaredQuery{}, err
	}

	return models.DeclaredQuery{
		Collection: collection,
		Query: models.Query{
			Name:         name,
			Description:  description,
			ParamsSchema: json.RawMessage(paramsSchema),
			Pipeline:     json.RawMessage(pipeline),
		},
	}, nil
}

// rawOrFallback keeps an absent JSON field from being stored as the empty
// string. The empty string is not valid JSON, so a later decode of that column
// would fail on a row that is otherwise perfectly well formed.
func rawOrFallback(raw json.RawMessage, fallback string) string {
	if len(raw) == 0 {
		return fallback
	}

	return string(raw)
}
