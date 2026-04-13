package models

import (
	"time"

	"github.com/google/uuid"
)

// ─── User ─────────────────────────────────────────────────────────────────────

type User struct {
	ID           uuid.UUID `json:"id" db:"id"`
	Email        string    `json:"email" db:"email"`
	PasswordHash string    `json:"-" db:"password_hash"`
	Name         string    `json:"name" db:"name"`
	ShardKey     int       `json:"shard_key" db:"shard_key"` // determines which PG shard
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
}

// ─── Product (stored in MongoDB) ──────────────────────────────────────────────

type Product struct {
	ID          string            `json:"id" bson:"_id,omitempty"`
	Name        string            `json:"name" bson:"name"`
	Description string            `json:"description" bson:"description"`
	Price       float64           `json:"price" bson:"price"`
	Stock       int               `json:"stock" bson:"stock"`
	Category    string            `json:"category" bson:"category"`
	Tags        []string          `json:"tags" bson:"tags"`
	Attributes  map[string]string `json:"attributes" bson:"attributes"`
	CreatedAt   time.Time         `json:"created_at" bson:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at" bson:"updated_at"`
}

// ─── Order (stored in sharded PostgreSQL) ─────────────────────────────────────

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusConfirmed OrderStatus = "confirmed"
	OrderStatusShipped   OrderStatus = "shipped"
	OrderStatusDelivered OrderStatus = "delivered"
	OrderStatusCancelled OrderStatus = "cancelled"
)

type Order struct {
	ID         uuid.UUID   `json:"id" db:"id"`
	UserID     uuid.UUID   `json:"user_id" db:"user_id"`
	Status     OrderStatus `json:"status" db:"status"`
	TotalPrice float64     `json:"total_price" db:"total_price"`
	ShardKey   int         `json:"shard_key" db:"shard_key"`
	CreatedAt  time.Time   `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at" db:"updated_at"`
	Items      []OrderItem `json:"items"`
}

type OrderItem struct {
	ID        uuid.UUID `json:"id" db:"id"`
	OrderID   uuid.UUID `json:"order_id" db:"order_id"`
	ProductID string    `json:"product_id" db:"product_id"`
	Quantity  int       `json:"quantity" db:"quantity"`
	UnitPrice float64   `json:"unit_price" db:"unit_price"`
}

// ─── Cart (stored in Redis) ────────────────────────────────────────────────────

type CartItem struct {
	ProductID string  `json:"product_id"`
	Name      string  `json:"name"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unit_price"`
}

type Cart struct {
	UserID    string     `json:"user_id"`
	Items     []CartItem `json:"items"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// ─── Request / Response DTOs ──────────────────────────────────────────────────

type RegisterRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=8"`
	Name     string `json:"name" binding:"required"`
}

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type LoginResponse struct {
	Token string `json:"token"`
	User  *User  `json:"user"`
}

type CreateOrderRequest struct {
	Items []OrderItemRequest `json:"items" binding:"required,min=1"`
}

type OrderItemRequest struct {
	ProductID string `json:"product_id" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required,min=1"`
}

type AddToCartRequest struct {
	ProductID string `json:"product_id" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required,min=1"`
}
