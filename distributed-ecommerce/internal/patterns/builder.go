// Package patterns contains pure design pattern implementations that are
// reused across the application.
//
// Builder Pattern
// ───────────────
// Constructs complex objects step-by-step, separating construction from
// representation. Prevents "telescoping constructor" anti-pattern where
// functions have 10+ parameters.
//
// Used for:
//   - OrderBuilder: assembles an Order with items, pricing, and validation
//   - ProductBuilder: assembles a Product with optional fields
//   - QueryBuilder: assembles MongoDB/PostgreSQL query predicates
//
// Tradeoff: slightly more verbose call sites vs. much safer construction
// (impossible to forget required fields, impossible to set invalid state).
package patterns

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ─── Order Builder ────────────────────────────────────────────────────────────

// OrderBuilder constructs an Order step-by-step with validation at Build().
type OrderBuilder struct {
	userID     uuid.UUID
	items      []OrderItemSpec
	totalPrice float64
	status     string
	errs       []error
}

// OrderItemSpec is the input spec for a single order line.
type OrderItemSpec struct {
	ProductID string
	Quantity  int
	UnitPrice float64
}

// BuiltOrder is the validated output of OrderBuilder.Build().
type BuiltOrder struct {
	UserID     uuid.UUID
	Items      []OrderItemSpec
	TotalPrice float64
	Status     string
	CreatedAt  time.Time
}

// NewOrderBuilder starts building an order for the given user.
func NewOrderBuilder(userID uuid.UUID) *OrderBuilder {
	if userID == uuid.Nil {
		return &OrderBuilder{errs: []error{errors.New("userID is required")}}
	}
	return &OrderBuilder{userID: userID, status: "pending"}
}

// AddItem appends a line item. Validates quantity and price.
func (b *OrderBuilder) AddItem(productID string, quantity int, unitPrice float64) *OrderBuilder {
	if productID == "" {
		b.errs = append(b.errs, fmt.Errorf("item %d: productID is empty", len(b.items)+1))
		return b
	}
	if quantity <= 0 {
		b.errs = append(b.errs, fmt.Errorf("item %s: quantity must be > 0", productID))
		return b
	}
	if unitPrice < 0 {
		b.errs = append(b.errs, fmt.Errorf("item %s: unitPrice must be >= 0", productID))
		return b
	}
	b.items = append(b.items, OrderItemSpec{
		ProductID: productID,
		Quantity:  quantity,
		UnitPrice: unitPrice,
	})
	b.totalPrice += unitPrice * float64(quantity)
	return b
}

// WithStatus overrides the default "pending" status.
func (b *OrderBuilder) WithStatus(status string) *OrderBuilder {
	b.status = status
	return b
}

// Build validates and returns the constructed order, or an error.
func (b *OrderBuilder) Build() (*BuiltOrder, error) {
	if len(b.errs) > 0 {
		return nil, fmt.Errorf("order build errors: %v", b.errs)
	}
	if len(b.items) == 0 {
		return nil, errors.New("order must have at least one item")
	}
	return &BuiltOrder{
		UserID:     b.userID,
		Items:      b.items,
		TotalPrice: b.totalPrice,
		Status:     b.status,
		CreatedAt:  time.Now(),
	}, nil
}

// ─── Product Builder ──────────────────────────────────────────────────────────

// ProductBuilder constructs a Product with optional fields.
type ProductBuilder struct {
	name        string
	description string
	price       float64
	stock       int
	category    string
	tags        []string
	attributes  map[string]string
	errs        []error
}

func NewProductBuilder(name string, price float64, category string) *ProductBuilder {
	b := &ProductBuilder{
		name:       name,
		price:      price,
		category:   category,
		attributes: make(map[string]string),
	}
	if name == "" {
		b.errs = append(b.errs, errors.New("name is required"))
	}
	if price < 0 {
		b.errs = append(b.errs, errors.New("price must be >= 0"))
	}
	if category == "" {
		b.errs = append(b.errs, errors.New("category is required"))
	}
	return b
}

func (b *ProductBuilder) WithDescription(d string) *ProductBuilder {
	b.description = d
	return b
}

func (b *ProductBuilder) WithStock(s int) *ProductBuilder {
	if s < 0 {
		b.errs = append(b.errs, errors.New("stock must be >= 0"))
		return b
	}
	b.stock = s
	return b
}

func (b *ProductBuilder) WithTags(tags ...string) *ProductBuilder {
	b.tags = append(b.tags, tags...)
	return b
}

func (b *ProductBuilder) WithAttribute(key, value string) *ProductBuilder {
	b.attributes[key] = value
	return b
}

// BuiltProduct is the validated output of ProductBuilder.Build().
type BuiltProduct struct {
	Name        string
	Description string
	Price       float64
	Stock       int
	Category    string
	Tags        []string
	Attributes  map[string]string
}

func (b *ProductBuilder) Build() (*BuiltProduct, error) {
	if len(b.errs) > 0 {
		return nil, fmt.Errorf("product build errors: %v", b.errs)
	}
	return &BuiltProduct{
		Name:        b.name,
		Description: b.description,
		Price:       b.price,
		Stock:       b.stock,
		Category:    b.category,
		Tags:        b.tags,
		Attributes:  b.attributes,
	}, nil
}

// ─── Query Builder ────────────────────────────────────────────────────────────

// QueryBuilder assembles SQL WHERE clauses and args for PostgreSQL queries.
// Prevents SQL injection by always using parameterised queries.
type QueryBuilder struct {
	conditions []string
	args       []any
	orderBy    string
	limit      int
	offset     int
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{limit: 20}
}

// Where adds a condition. placeholder is the $N positional arg.
func (q *QueryBuilder) Where(condition string, value any) *QueryBuilder {
	q.args = append(q.args, value)
	// Replace ? with $N
	q.conditions = append(q.conditions, fmt.Sprintf(condition, len(q.args)))
	return q
}

func (q *QueryBuilder) OrderBy(col string, desc bool) *QueryBuilder {
	dir := "ASC"
	if desc {
		dir = "DESC"
	}
	q.orderBy = fmt.Sprintf("ORDER BY %s %s", col, dir)
	return q
}

func (q *QueryBuilder) Limit(n int) *QueryBuilder  { q.limit = n; return q }
func (q *QueryBuilder) Offset(n int) *QueryBuilder { q.offset = n; return q }

// Build returns the WHERE clause, ORDER BY, LIMIT/OFFSET, and args.
func (q *QueryBuilder) Build() (where string, orderBy string, limitOffset string, args []any) {
	if len(q.conditions) > 0 {
		where = "WHERE "
		for i, c := range q.conditions {
			if i > 0 {
				where += " AND "
			}
			where += c
		}
	}
	orderBy = q.orderBy
	args = append(q.args, q.limit, q.offset)
	limitOffset = fmt.Sprintf("LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	return
}
