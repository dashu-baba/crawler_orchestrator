package store

import (
	"bytes"
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// NewClient creates a MinIO client and confirms bucket exists before
// returning, so a bad endpoint/credentials/bucket fails here instead of on
// the first real write.
func NewClient(ctx context.Context, endpoint, accessKey, secretKey, bucket string, useSSL bool) (*minio.Client, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("checking bucket %s: %w", bucket, err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %s does not exist", bucket)
	}

	return client, nil
}

// Write uploads payload to bucket at key idemKey+".json", overwriting any
// existing object at that key. Safe to call more than once for the same
// idemKey.
func Write(ctx context.Context, client *minio.Client, bucket, idemKey string, payload []byte) error {
	key := idemKey + ".json"

	_, err := client.PutObject(ctx, bucket, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("writing object %s: %w", key, err)
	}

	return nil
}
