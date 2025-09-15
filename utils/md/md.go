package md

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetMongoInstance(ctx context.Context, host, username, passwd, authSource string) (*mongo.Client, error) {
	md, err := mongo.Connect(ctx, options.Client().ApplyURI(host), options.Client().SetAuth(options.Credential{
		Username:   username,
		Password:   passwd,
		AuthSource: authSource,
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	return md, nil
}
