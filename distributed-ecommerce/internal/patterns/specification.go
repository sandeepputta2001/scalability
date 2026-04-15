// Specification Pattern
// ──────────────────────
// Encapsulates a business rule as a composable, reusable object.
// Specifications can be combined with AND, OR, NOT operators.
//
// Without specifications:
//
//	func (r *ProductRepo) ListAvailableElectronicsUnder500(ctx) ([]*Product, error) {
//	  // hardcoded query — not reusable, not testable in isolation
//	}
//	// Need a new method for every combination of filters
//
// With specifications:
//
//	spec := InCategory("electronics").And(PriceBelow(500)).And(InStock())
//	products, err := repo.FindBySpec(ctx, spec)
//	// Compose any combination without new methods
//
// Tradeoff:
//
//	✓ Reusable: combine specs without new repository methods
//	✓ Testable: test each spec in isolation
//	✓ Readable: business rules expressed in domain language
//	✗ Performance: complex specs may generate inefficient queries
//	✗ Leaky abstraction: MongoDB/SQL-specific optimisations are harder
package patterns

import (
	"fmt"
	"strings"
)

// Specification defines a predicate over a domain object.
type Specification interface {
	// IsSatisfiedBy checks if the spec is satisfied in-memory (for filtering).
	IsSatisfiedBy(candidate any) bool
	// ToMongoFilter returns a MongoDB BSON filter map.
	ToMongoFilter() map[string]any
	// ToSQLWhere returns a SQL WHERE clause fragment and args.
	ToSQLWhere() (string, []any)
	// Description returns a human-readable description of the spec.
	Description() string
}

// ─── Base Specifications ──────────────────────────────────────────────────────

// CategorySpec matches products in a specific category.
type CategorySpec struct {
	Category string
}

func InCategory(category string) *CategorySpec {
	return &CategorySpec{Category: category}
}

func (s *CategorySpec) IsSatisfiedBy(candidate any) bool {
	if p, ok := candidate.(interface{ GetCategory() string }); ok {
		return p.GetCategory() == s.Category
	}
	return false
}

func (s *CategorySpec) ToMongoFilter() map[string]any {
	return map[string]any{"category": s.Category}
}

func (s *CategorySpec) ToSQLWhere() (string, []any) {
	return "category = $1", []any{s.Category}
}

func (s *CategorySpec) Description() string {
	return fmt.Sprintf("category=%s", s.Category)
}

// PriceBelowSpec matches products with price below a threshold.
type PriceBelowSpec struct {
	MaxPrice float64
}

func PriceBelow(max float64) *PriceBelowSpec {
	return &PriceBelowSpec{MaxPrice: max}
}

func (s *PriceBelowSpec) IsSatisfiedBy(candidate any) bool {
	if p, ok := candidate.(interface{ GetPrice() float64 }); ok {
		return p.GetPrice() < s.MaxPrice
	}
	return false
}

func (s *PriceBelowSpec) ToMongoFilter() map[string]any {
	return map[string]any{"price": map[string]any{"$lt": s.MaxPrice}}
}

func (s *PriceBelowSpec) ToSQLWhere() (string, []any) {
	return "price < $1", []any{s.MaxPrice}
}

func (s *PriceBelowSpec) Description() string {
	return fmt.Sprintf("price<%.2f", s.MaxPrice)
}

// InStockSpec matches products with stock > 0.
type InStockSpec struct{}

func InStock() *InStockSpec { return &InStockSpec{} }

func (s *InStockSpec) IsSatisfiedBy(candidate any) bool {
	if p, ok := candidate.(interface{ GetStock() int }); ok {
		return p.GetStock() > 0
	}
	return false
}

func (s *InStockSpec) ToMongoFilter() map[string]any {
	return map[string]any{"stock": map[string]any{"$gt": 0}}
}

func (s *InStockSpec) ToSQLWhere() (string, []any) {
	return "stock > 0", nil
}

func (s *InStockSpec) Description() string { return "in_stock" }

// HasTagSpec matches products that have a specific tag.
type HasTagSpec struct {
	Tag string
}

func HasTag(tag string) *HasTagSpec { return &HasTagSpec{Tag: tag} }

func (s *HasTagSpec) IsSatisfiedBy(candidate any) bool {
	if p, ok := candidate.(interface{ GetTags() []string }); ok {
		for _, t := range p.GetTags() {
			if t == s.Tag {
				return true
			}
		}
	}
	return false
}

func (s *HasTagSpec) ToMongoFilter() map[string]any {
	return map[string]any{"tags": s.Tag}
}

func (s *HasTagSpec) ToSQLWhere() (string, []any) {
	return "$1 = ANY(tags)", []any{s.Tag}
}

func (s *HasTagSpec) Description() string { return fmt.Sprintf("tag=%s", s.Tag) }

// ─── Composite Specifications ─────────────────────────────────────────────────

// AndSpec combines two specs with logical AND.
type AndSpec struct {
	Left, Right Specification
}

func (s *AndSpec) IsSatisfiedBy(candidate any) bool {
	return s.Left.IsSatisfiedBy(candidate) && s.Right.IsSatisfiedBy(candidate)
}

func (s *AndSpec) ToMongoFilter() map[string]any {
	return map[string]any{"$and": []any{s.Left.ToMongoFilter(), s.Right.ToMongoFilter()}}
}

func (s *AndSpec) ToSQLWhere() (string, []any) {
	lClause, lArgs := s.Left.ToSQLWhere()
	rClause, rArgs := s.Right.ToSQLWhere()
	// Re-number right args to avoid $1 collision
	offset := len(lArgs)
	for i := 1; i <= len(rArgs); i++ {
		rClause = strings.ReplaceAll(rClause, fmt.Sprintf("$%d", i), fmt.Sprintf("$%d", i+offset))
	}
	return fmt.Sprintf("(%s AND %s)", lClause, rClause), append(lArgs, rArgs...)
}

func (s *AndSpec) Description() string {
	return fmt.Sprintf("(%s AND %s)", s.Left.Description(), s.Right.Description())
}

// OrSpec combines two specs with logical OR.
type OrSpec struct {
	Left, Right Specification
}

func (s *OrSpec) IsSatisfiedBy(candidate any) bool {
	return s.Left.IsSatisfiedBy(candidate) || s.Right.IsSatisfiedBy(candidate)
}

func (s *OrSpec) ToMongoFilter() map[string]any {
	return map[string]any{"$or": []any{s.Left.ToMongoFilter(), s.Right.ToMongoFilter()}}
}

func (s *OrSpec) ToSQLWhere() (string, []any) {
	lClause, lArgs := s.Left.ToSQLWhere()
	rClause, rArgs := s.Right.ToSQLWhere()
	offset := len(lArgs)
	for i := 1; i <= len(rArgs); i++ {
		rClause = strings.ReplaceAll(rClause, fmt.Sprintf("$%d", i), fmt.Sprintf("$%d", i+offset))
	}
	return fmt.Sprintf("(%s OR %s)", lClause, rClause), append(lArgs, rArgs...)
}

func (s *OrSpec) Description() string {
	return fmt.Sprintf("(%s OR %s)", s.Left.Description(), s.Right.Description())
}

// NotSpec negates a spec.
type NotSpec struct {
	Inner Specification
}

func (s *NotSpec) IsSatisfiedBy(candidate any) bool {
	return !s.Inner.IsSatisfiedBy(candidate)
}

func (s *NotSpec) ToMongoFilter() map[string]any {
	return map[string]any{"$nor": []any{s.Inner.ToMongoFilter()}}
}

func (s *NotSpec) ToSQLWhere() (string, []any) {
	clause, args := s.Inner.ToSQLWhere()
	return fmt.Sprintf("NOT (%s)", clause), args
}

func (s *NotSpec) Description() string {
	return fmt.Sprintf("NOT(%s)", s.Inner.Description())
}

// ─── Fluent API helpers ───────────────────────────────────────────────────────

// And combines this spec with another using AND.
func (s *CategorySpec) And(other Specification) Specification {
	return &AndSpec{Left: s, Right: other}
}

func (s *PriceBelowSpec) And(other Specification) Specification {
	return &AndSpec{Left: s, Right: other}
}

func (s *InStockSpec) And(other Specification) Specification {
	return &AndSpec{Left: s, Right: other}
}

func (s *HasTagSpec) And(other Specification) Specification {
	return &AndSpec{Left: s, Right: other}
}

func (s *AndSpec) And(other Specification) Specification {
	return &AndSpec{Left: s, Right: other}
}

func (s *OrSpec) And(other Specification) Specification {
	return &AndSpec{Left: s, Right: other}
}
