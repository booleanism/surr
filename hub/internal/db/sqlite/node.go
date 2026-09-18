package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
)

type nodes struct {
	*db.Transaction
}

func NewNodes(txFactory func() *db.Transaction) nodes {
	return nodes{
		Transaction: txFactory(),
	}
}

func (r nodes) InsertNodes(ctx context.Context, nodes []models.Node) error {
	if len(nodes) == 0 {
		return fmt.Errorf("insert nodes: nothing to insert")
	}

	valueStrings := make([]string, 0, len(nodes))
	valueArgs := make([]any, 0, len(nodes)*5)

	for _, n := range nodes {
		valueStrings = append(valueStrings, "(?, ?, ?, ?)")
		valueArgs = append(valueArgs, n.ID, n.Bitrate, n.Width, n.Height)
	}

	stmt := fmt.Sprintf("INSERT INTO nodes (id, bitrate, width, height) VALUES %s", strings.Join(valueStrings, ", "))

	_, err := r.DB.ExecContext(ctx, stmt, valueArgs...)
	return err
}

func (r nodes) DeleteNodes(ctx context.Context, nodes []models.Node) error {
	if len(nodes) == 0 {
		return fmt.Errorf("remove nodes: nothing to remove")
	}

	placeholders := make([]string, len(nodes))
	nodeIDs := make([]any, len(nodes))

	for i, n := range nodes {
		placeholders[i] = "?"
		nodeIDs[i] = n.ID
	}

	stmt := fmt.Sprintf("DELETE FROM nodes WHERE id IN (%s)", strings.Join(placeholders, ", "))

	_, err := r.DB.ExecContext(ctx, stmt, nodeIDs...)
	return err
}

func (r nodes) SelectAllNodesByID(ctx context.Context, nodes *[]models.Node) (int, error) {
	idPlaceholder := ""
	idsArgs := []any{}
	for _, n := range (*nodes)[:len(*nodes)-1] {
		idPlaceholder = fmt.Sprintf("%s?, ", idPlaceholder)
		idsArgs = append(idsArgs, n.ID)
	}

	idsArgs = append(idsArgs, (*nodes)[len(*nodes)-1].ID)
	stmt := fmt.Sprintf("SELECT id, bitrate, width, height FROM nodes WHERE id IN (%s ?)", idPlaceholder)
	rws, err := r.DB.QueryContext(ctx, stmt, idsArgs...)
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	i := 0
	for rws.Next() {
		(*nodes) = append((*nodes), models.Node{})
		err = rws.Scan(&(*nodes)[i].ID, &(*nodes)[i].Bitrate, &(*nodes)[i].Width, &(*nodes)[i].Height)
		if err != nil {
			return i, err
		}

		i++
	}

	err = rws.Err()
	if err != nil {
		return i, err
	}

	return i, nil
}

func (r nodes) SelectAllNodes(ctx context.Context, nodes *[]models.Node) (int, error) {
	rws, err := r.DB.QueryContext(ctx, "SELECT id, bitrate, width, height FROM nodes")
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	i := 0
	for rws.Next() {
		(*nodes) = append((*nodes), models.Node{})
		err = rws.Scan(&(*nodes)[i].ID, &(*nodes)[i].Bitrate, &(*nodes)[i].Width, &(*nodes)[i].Height)
		if err != nil {
			return i, err
		}

		i++
	}

	err = rws.Err()
	if err != nil {
		return i, err
	}

	return i, nil
}
