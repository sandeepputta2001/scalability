// Initialize MongoDB replica set
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo-primary:27017", priority: 2 },
    { _id: 1, host: "mongo-secondary1:27017", priority: 1 },
    { _id: 2, host: "mongo-secondary2:27017", priority: 1 },
  ],
});

// Wait for election
sleep(5000);

// Create catalog indexes
db = db.getSiblingDB("catalog");
db.products.createIndex({ category: 1 });
db.products.createIndex({ tags: 1 });
db.products.createIndex({ name: "text", description: "text" });

print("MongoDB replica set initialized and indexes created.");
