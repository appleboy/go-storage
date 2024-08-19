package minio

import (
	"context"
	"log"
	"testing"

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
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
}
