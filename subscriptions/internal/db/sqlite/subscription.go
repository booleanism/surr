package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type subscription struct {
	*db.Transaction
}

func NewSubscription(txFactory db.TXFactoryFunc) subscription {
	return subscription{
		Transaction: txFactory(),
	}
}

func (s subscription) SelectSubscriptionsByUser(ctx context.Context, userID uuid.UUID, buf *[]models.Subscription) (int, error) {
	stmt := `
		SELECT id, user_id FROM subscriptions WHERE user_id = ?
	`

	rws, err := s.DB.QueryContext(ctx, stmt, userID)
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	return s.scanRows(rws, func(i int) []any {
		if i >= cap(*buf) {
			return nil
		}

		*buf = append(*buf, models.Subscription{})
		return []any{&(*buf)[i].ID, &(*buf)[i].User.ID}
	})
}

func (s subscription) InsertSubscriptions(ctx context.Context, subsNodesList []models.SubscribedNodes) error {
	cleanup, err := s.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer cleanup()

	subValueStrings := make([]string, 0, len(subsNodesList))
	subValueArgs := make([]any, 0, len(subsNodesList)*2)

	nodeValueStrings := make([]string, 0)
	nodeValueArgs := make([]any, 0)

	for _, subsNodes := range subsNodesList {
		subValueStrings = append(subValueStrings, "(?, ?)")
		subValueArgs = append(subValueArgs, subsNodes.Subscription.ID, subsNodes.User.ID)

		for _, node := range subsNodes.Nodes {
			nodeValueStrings = append(nodeValueStrings, "(?, ?)")
			nodeValueArgs = append(nodeValueArgs, subsNodes.Subscription.ID, node.ID)
		}
	}

	subStmt := fmt.Sprintf("INSERT INTO subscriptions (id, user_id) VALUES %s", strings.Join(subValueStrings, ", "))
	if _, err = s.DB.ExecContext(ctx, subStmt, subValueArgs...); err != nil {
		_ = s.Rollback()
		return err
	}

	nodeStmt := fmt.Sprintf("INSERT INTO subscribed_nodes (subscription_id, node_id) VALUES %s", strings.Join(nodeValueStrings, ", "))
	if _, err = s.DB.ExecContext(ctx, nodeStmt, nodeValueArgs...); err != nil {
		_ = s.Rollback()
		return err
	}

	_ = s.Commit()
	return nil
}

func (s subscription) scanRows(rws *sql.Rows, fn func(i int) []any) (int, error) {
	i := 0
	for rws.Next() {
		dest := fn(i)
		if dest == nil {
			break
		}

		err := rws.Scan(dest...)
		if err != nil {
			return i, err
		}

		i++
	}

	if err := rws.Err(); err != nil {
		return i, err
	}

	return i, nil
}

func (s subscription) DeleteSubscriptions(ctx context.Context, subsID []models.Subscription) error {
	idPlaceholder := ""
	idsArgs := []any{}

	for _, id := range subsID[:len(subsID)-1] {
		idPlaceholder = fmt.Sprintf("%s?, ", idPlaceholder)
		idsArgs = append(idsArgs, id.ID)
	}

	idsArgs = append(idsArgs, subsID[len(subsID)-1].ID)
	stmt := fmt.Sprintf("DELETE FROM subscriptions WHERE id IN (%s ?)", idPlaceholder)
	_, err := s.DB.ExecContext(ctx, stmt, idsArgs...)
	return err
}

func (s subscription) SelectSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error) {
	rws, err := s.DB.QueryContext(ctx, "SELECT node_id FROM subscribed_nodes WHERE subscription_id = ?", sub.ID)
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	return s.scanRows(rws, func(i int) []any {
		if i >= cap(*nodes) {
			return nil
		}

		*nodes = append(*nodes, models.Node{})
		return []any{&(*nodes)[i].ID}
	})
}

func (s subscription) SelectAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error) {
	rws, err := s.DB.QueryContext(ctx, "SELECT node_id FROM subscribed_nodes")
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	return s.scanRows(rws, func(i int) []any {
		if i >= cap(*nodes) {
			return nil
		}

		*nodes = append(*nodes, models.Node{})
		return []any{&(*nodes)[i].ID}
	})
}

func (s subscription) SelectSubscriptionByNode(ctx context.Context, node models.Node, subs *[]models.Subscription) error {
	rw := s.DB.QueryRowContext(ctx, "SELECT subscription_id FROM subscribed_nodes WHERE node_id = ?", node.ID)
	(*subs) = append((*subs), models.Subscription{})
	err := rw.Scan(&(*subs)[0].ID)
	if err != nil {
		return err
	}

	return rw.Err()
}
