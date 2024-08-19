package minio

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func getMinio() (*minio.MinioContainer, error) {
	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}

	return minioContainer, err
}

func TestCreateBucket(t *testing.T) {
	minioContainer, err := getMinio()
	assert.NoError(t, err)

	conStr, err := minioContainer.ConnectionString(context.Background())
	assert.NoError(t, err)

	client, err := NewEngine(conStr, "minioadmin", "minioadmin", false, true, "us-east-1")
	assert.NoError(t, err)

	// create a bucket
	err = client.CreateBucket(context.Background(), "testbucket", "us-east-1")
	assert.NoError(t, err)

	defer func() {
		err := minioContainer.Terminate(context.Background())
		assert.NoError(t, err)
	}()
}
