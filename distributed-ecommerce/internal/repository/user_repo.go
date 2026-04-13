package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/db"
	"github.com/distributed-ecommerce/internal/models"
)

// UserRepository handles user persistence across shards.
// Sharding key: user email (consistent hash → shard index stored on the record).
type UserRepository struct {
	sm  *db.ShardManager
	log *zap.Logger
}

func NewUserRepository(sm *db.ShardManager, log *zap.Logger) *UserRepository {
	return &UserRepository{sm: sm, log: log}
}

// Create inserts a new user into the appropriate shard.
func (r *UserRepository) Create(ctx context.Context, user *models.User) error {
	shard := r.sm.ShardForKey(user.Email)
	user.ShardKey = shard.ID
	user.ID = uuid.New()
	user.CreatedAt = time.Now()

	_, err := shard.Primary.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, name, shard_key, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		user.ID, user.Email, user.PasswordHash, user.Name, user.ShardKey, user.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create user: %w", err)
	}
	r.log.Debug("user created", zap.String("id", user.ID.String()), zap.Int("shard", shard.ID))
	return nil
}

// GetByEmail finds a user by email — routes to the correct shard via hash.
func (r *UserRepository) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	shard := r.sm.ShardForKey(email)
	// reads go to a replica
	row := shard.ReplicaPool().QueryRow(ctx, `
		SELECT id, email, password_hash, name, shard_key, created_at
		FROM users WHERE email = $1`, email)

	var u models.User
	if err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.ShardKey, &u.CreatedAt); err != nil {
		return nil, fmt.Errorf("get user by email: %w", err)
	}
	return &u, nil
}

// GetByID finds a user by ID. Because we store shard_key on the record we need
// to scatter-gather across all shards (or accept the shard_key as a hint).
func (r *UserRepository) GetByID(ctx context.Context, id uuid.UUID, shardKey int) (*models.User, error) {
	shard := r.sm.ShardByIndex(shardKey)
	row := shard.ReplicaPool().QueryRow(ctx, `
		SELECT id, email, password_hash, name, shard_key, created_at
		FROM users WHERE id = $1`, id)

	var u models.User
	if err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.ShardKey, &u.CreatedAt); err != nil {
		return nil, fmt.Errorf("get user by id: %w", err)
	}
	return &u, nil
}
