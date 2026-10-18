package mongo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	mongoDriver "go.mongodb.org/mongo-driver/mongo"
)

// Aggregate runs a pipeline the usecase has already validated and returns the
// documents as a JSON array.
//
// The caps are enforced while reading rather than after: a pipeline without a
// $limit would otherwise be able to pull a whole collection into memory before
// anyone noticed, which is the failure this bound exists to prevent.
//
// The pipeline is decoded into bson.D and not into a map, because bson.D keeps
// key order and the operand order of $sort is part of what a query means.
func (c *Client) Aggregate(ctx context.Context, collection string, pipeline []byte, maxDocs, maxBytes int) (json.RawMessage, error) {
	var stages []bson.D

	err := bson.UnmarshalExtJSON(pipeline, false, &stages)
	if err != nil {
		return nil, fmt.Errorf("decode pipeline: %w", err)
	}

	cursor, err := c.Database.Collection(collection).Aggregate(ctx, mongoDriver.Pipeline(stages))
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}

	defer func() {
		_ = cursor.Close(ctx)
	}()

	out := make([]byte, 0, 4096)
	out = append(out, '[')

	count := 0

	for cursor.Next(ctx) {
		if count >= maxDocs {
			return nil, fmt.Errorf("%w: more than %d documents", models.ErrQueryTooLarge, maxDocs)
		}

		doc, marshalErr := bson.MarshalExtJSON(cursor.Current, false, false)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal document: %w", marshalErr)
		}

		if count > 0 {
			out = append(out, ',')
		}

		out = append(out, doc...)

		count++

		if len(out) > maxBytes {
			return nil, fmt.Errorf("%w: more than %d bytes", models.ErrQueryTooLarge, maxBytes)
		}
	}

	err = cursor.Err()
	if err != nil {
		return nil, fmt.Errorf("read documents: %w", err)
	}

	out = append(out, ']')

	return out, nil
}
