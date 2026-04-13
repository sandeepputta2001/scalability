package repository

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/models"
	mongoClient "github.com/distributed-ecommerce/internal/mongo"
)

const productCollection = "products"

// ProductRepository stores products in MongoDB.
// Reads use secondaryPreferred (configured at the driver level).
type ProductRepository struct {
	col *mongo.Collection
	log *zap.Logger
}

func NewProductRepository(mc *mongoClient.Client, log *zap.Logger) *ProductRepository {
	col := mc.Collection(productCollection)
	// ensure indexes
	ctx := context.Background()
	_, _ = col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "category", Value: 1}}},
		{Keys: bson.D{{Key: "tags", Value: 1}}},
		{Keys: bson.D{{Key: "name", Value: "text"}, {Key: "description", Value: "text"}}},
	})
	return &ProductRepository{col: col, log: log}
}

func (r *ProductRepository) Create(ctx context.Context, p *models.Product) error {
	p.CreatedAt = time.Now()
	p.UpdatedAt = time.Now()
	res, err := r.col.InsertOne(ctx, p)
	if err != nil {
		return fmt.Errorf("create product: %w", err)
	}
	if oid, ok := res.InsertedID.(primitive.ObjectID); ok {
		p.ID = oid.Hex()
	}
	return nil
}

func (r *ProductRepository) GetByID(ctx context.Context, id string) (*models.Product, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, fmt.Errorf("invalid product id: %w", err)
	}
	var p models.Product
	if err := r.col.FindOne(ctx, bson.M{"_id": oid}).Decode(&p); err != nil {
		return nil, fmt.Errorf("get product: %w", err)
	}
	return &p, nil
}

func (r *ProductRepository) List(ctx context.Context, category string, page, pageSize int) ([]*models.Product, error) {
	filter := bson.M{}
	if category != "" {
		filter["category"] = category
	}

	skip := int64((page - 1) * pageSize)
	limit := int64(pageSize)
	opts := options.Find().SetSkip(skip).SetLimit(limit).SetSort(bson.D{{Key: "created_at", Value: -1}})

	cur, err := r.col.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}
	defer cur.Close(ctx)

	var products []*models.Product
	if err := cur.All(ctx, &products); err != nil {
		return nil, err
	}
	return products, nil
}

func (r *ProductRepository) Search(ctx context.Context, query string) ([]*models.Product, error) {
	filter := bson.M{"$text": bson.M{"$search": query}}
	cur, err := r.col.Find(ctx, filter, options.Find().SetLimit(20))
	if err != nil {
		return nil, fmt.Errorf("search products: %w", err)
	}
	defer cur.Close(ctx)

	var products []*models.Product
	if err := cur.All(ctx, &products); err != nil {
		return nil, err
	}
	return products, nil
}

func (r *ProductRepository) UpdateStock(ctx context.Context, id string, delta int) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid product id: %w", err)
	}
	res, err := r.col.UpdateOne(ctx,
		bson.M{"_id": oid, "stock": bson.M{"$gte": -delta}},
		bson.M{
			"$inc": bson.M{"stock": delta},
			"$set": bson.M{"updated_at": time.Now()},
		},
	)
	if err != nil {
		return fmt.Errorf("update stock: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("insufficient stock or product not found")
	}
	return nil
}
