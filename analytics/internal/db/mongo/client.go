// Package mongo owns the analytics service's connection to the document store.
//
// It exists as its own package for one reason: the node is the only MongoDB
// client in the system today, and it connects with root credentials. Moving the
// read path into this service is the first step of taking those credentials away
// from the Python side, so the connection is written once, here, where the
// service that will own it can be the one to change it.
package mongo

import (
	"context"
	"fmt"
	"time"

	mongoDriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// connectTimeout bounds both the initial dial and the ping that proves the
// credentials work. Without a ping, mongo.Connect succeeds against an address
// that will never answer and the failure surfaces on the first query instead of
// at startup.
const connectTimeout = 10 * time.Second

// Client is a connection to one database.
//
// The embedded *mongoDriver.Database is what the executor will aggregate
// against; Close is the only thing that needs the underlying client, which is
// kept separately because Database does not expose it.
type Client struct {
	*mongoDriver.Database
	client *mongoDriver.Client
}

// New dials url, verifies the connection, and returns the named database.
//
// The URL carries credentials, so callers must not log it: log the redacted form
// (logging.RedactURL) the way the broker URL already is.
func New(ctx context.Context, url, database string) (*Client, error) {
	opts := options.Client().ApplyURI(url).SetConnectTimeout(connectTimeout)

	client, err := mongoDriver.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if pingErr := client.Ping(pingCtx, nil); pingErr != nil {
		// The connection is useless, so close it rather than leaking the
		// background goroutines Connect started. The caller has no handle to do
		// that itself.
		_ = client.Disconnect(context.WithoutCancel(ctx))

		return nil, fmt.Errorf("mongo ping: %w", pingErr)
	}

	return &Client{
		Database: client.Database(database),
		client:   client,
	}, nil
}

// Close releases the connection. It is registered with logging.Cleanups, which
// runs it on shutdown, so it takes a context of its own: the context that
// triggered the drain is already cancelled by the time this is called.
func (c *Client) Close() error {
	return c.client.Disconnect(context.Background())
}
