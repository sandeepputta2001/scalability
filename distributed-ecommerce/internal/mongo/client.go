// Package mongo wraps the MongoDB driver with replica-set aware configuration.
// Reads use secondaryPreferred so they are served by secondaries when available,
// reducing load on the primary. Writes always go to the primary.
package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/config"
)

// Client wraps the mongo.Client with convenience accessors.
type Client struct {
	client *mongo.Client
	db     *mongo.Database
	log    *zap.Logger
}

// NewClient connects to the MongoDB replica set.
func NewClient(ctx context.Context, cfg config.MongoConfig, log *zap.Logger) (*Client, error) {
	rp, err := readPrefFromString(cfg.ReadPreference)
	if err != nil {
		return nil, err
	}

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetReadPreference(rp).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	if err := client.Ping(ctx, rp); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	log.Info("mongodb connected",
		zap.String("database", cfg.Database),
		zap.String("read_preference", cfg.ReadPreference))

	return &Client{
		client: client,
		db:     client.Database(cfg.Database),
		log:    log,
	}, nil
}

// Collection returns a collection handle.
func (c *Client) Collection(name string) *mongo.Collection {
	return c.db.Collection(name)
}

// Database returns the underlying database handle.
func (c *Client) Database() *mongo.Database {
	return c.db
}

// Disconnect closes the connection.
func (c *Client) Disconnect(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func readPrefFromString(s string) (*readpref.ReadPref, error) {
	switch s {
	case "primary":
		return readpref.Primary(), nil
	case "primaryPreferred":
		return readpref.PrimaryPreferred(), nil
	case "secondary":
		return readpref.Secondary(), nil
	case "secondaryPreferred":
		return readpref.SecondaryPreferred(), nil
	case "nearest":
		return readpref.Nearest(), nil
	default:
		return readpref.SecondaryPreferred(), nil
	}
}
