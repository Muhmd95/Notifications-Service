package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectMongoDB(mongoURI string) (*mongo.Client, error) {
	// Connect to MongoDB and apply 10 sec timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a new MongoDB client and connect to the server
	clientOptions := options.Client().ApplyURI(mongoURI)
	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	//ping to check if the connection is established
	if err := mongoClient.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return mongoClient, nil
}
