package minio

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
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

func TestDeleteFile(t *testing.T) {
	minioContainer, err := getMinio()
	assert.NoError(t, err)

	conStr, err := minioContainer.ConnectionString(context.Background())
	assert.NoError(t, err)

	client, err := NewEngine(conStr, "minioadmin", "minioadmin", false, true, "us-east-1")
	assert.NoError(t, err)

	// create a bucket
	err = client.CreateBucket(context.Background(), "testbucket", "us-east-1")
	assert.NoError(t, err)

	// upload a file
	content := []byte("test content")
	err = client.UploadFile(context.Background(), "testbucket", "testfile.txt", content, nil)
	assert.NoError(t, err)

	// delete the file
	err = client.DeleteFile(context.Background(), "testbucket", "testfile.txt")
	assert.NoError(t, err)

	// check if the file exists
	exists := client.FileExist(context.Background(), "testbucket", "testfile.txt")
	assert.False(t, exists)

	defer func() {
		err := minioContainer.Terminate(context.Background())
		assert.NoError(t, err)
	}()
}

func TestDownloadFile(t *testing.T) {
	minioContainer, err := getMinio()
	assert.NoError(t, err)

	conStr, err := minioContainer.ConnectionString(context.Background())
	assert.NoError(t, err)

	client, err := NewEngine(conStr, "minioadmin", "minioadmin", false, true, "us-east-1")
	assert.NoError(t, err)

	// create a bucket
	err = client.CreateBucket(context.Background(), "testbucket", "us-east-1")
	assert.NoError(t, err)

	// upload a file
	content := []byte("test content")
	err = client.UploadFile(context.Background(), "testbucket", "testfile.txt", content, nil)
	assert.NoError(t, err)

	// check if the targetFile exists
	exists := client.FileExist(context.Background(), "testbucket", "testfile.txt")
	assert.True(t, exists)

	// download the file
	tmp := t.TempDir()
	targetFile := filepath.Join(tmp, "file.txt")
	err = client.DownloadFile(context.Background(), "testbucket", "testfile.txt", targetFile)
	assert.NoError(t, err)

	_, err = os.Stat(targetFile)
	assert.NoError(t, err)

	defer func() {
		err := minioContainer.Terminate(context.Background())
		assert.NoError(t, err)
	}()
}

func TestGetContent(t *testing.T) {
	minioContainer, err := getMinio()
	assert.NoError(t, err)

	conStr, err := minioContainer.ConnectionString(context.Background())
	assert.NoError(t, err)

	client, err := NewEngine(conStr, "minioadmin", "minioadmin", false, true, "us-east-1")
	assert.NoError(t, err)

	// create a bucket
	err = client.CreateBucket(context.Background(), "testbucket", "us-east-1")
	assert.NoError(t, err)

	// upload a file
	content := []byte("test content")
	err = client.UploadFile(context.Background(), "testbucket", "testfile.txt", content, nil)
	assert.NoError(t, err)

	// get the content of the file
	fileContent, err := client.GetContent(context.Background(), "testbucket", "testfile.txt")
	assert.NoError(t, err)
	assert.Equal(t, content, fileContent)

	defer func() {
		err := minioContainer.Terminate(context.Background())
		assert.NoError(t, err)
	}()
}

func TestBucketExists(t *testing.T) {
	minioContainer, err := getMinio()
	assert.NoError(t, err)

	conStr, err := minioContainer.ConnectionString(context.Background())
	assert.NoError(t, err)

	client, err := NewEngine(conStr, "minioadmin", "minioadmin", false, true, "us-east-1")
	assert.NoError(t, err)

	// create a bucket
	err = client.CreateBucket(context.Background(), "testbucket", "us-east-1")
	assert.NoError(t, err)

	// check if the bucket exists
	exists, err := client.BucketExists(context.Background(), "testbucket")
	assert.NoError(t, err)
	assert.True(t, exists)

	// check if a non-existent bucket exists
	exists, err = client.BucketExists(context.Background(), "nonexistentbucket")
	assert.NoError(t, err)
	assert.False(t, exists)

	defer func() {
		err := minioContainer.Terminate(context.Background())
		assert.NoError(t, err)
	}()
}

func TestCopyFile(t *testing.T) {
	minioContainer, err := getMinio()
	assert.NoError(t, err)

	conStr, err := minioContainer.ConnectionString(context.Background())
	assert.NoError(t, err)

	client, err := NewEngine(conStr, "minioadmin", "minioadmin", false, true, "us-east-1")
	assert.NoError(t, err)

	// create source bucket
	err = client.CreateBucket(context.Background(), "sourcebucket", "us-east-1")
	assert.NoError(t, err)

	// create destination bucket
	err = client.CreateBucket(context.Background(), "destinationbucket", "us-east-1")
	assert.NoError(t, err)

	// upload a file to the source bucket
	content := []byte("test content")
	err = client.UploadFile(context.Background(), "sourcebucket", "testfile.txt", content, nil)
	assert.NoError(t, err)

	// copy the file from source bucket to destination bucket
	err = client.CopyFile(context.Background(), "sourcebucket", "testfile.txt", "destinationbucket", "testfile.txt")
	assert.NoError(t, err)

	// check if the file exists in the destination bucket
	exists := client.FileExist(context.Background(), "destinationbucket", "testfile.txt")
	assert.True(t, exists)

	defer func() {
		err := minioContainer.Terminate(context.Background())
		assert.NoError(t, err)
	}()
}

func TestUploadFileByReader(t *testing.T) {
	minioContainer, err := getMinio()
	assert.NoError(t, err)

	conStr, err := minioContainer.ConnectionString(context.Background())
	assert.NoError(t, err)

	client, err := NewEngine(conStr, "minioadmin", "minioadmin", false, true, "us-east-1")
	assert.NoError(t, err)

	// create a bucket
	err = client.CreateBucket(context.Background(), "testbucket", "us-east-1")
	assert.NoError(t, err)

	// upload a file using reader
	content := []byte("test content")
	reader := bytes.NewReader(content)
	err = client.UploadFileByReader(
		context.Background(), "testbucket", "testfile.txt",
		reader, "text/plain", int64(len(content)))
	assert.NoError(t, err)

	// check if the file exists
	exists := client.FileExist(context.Background(), "testbucket", "testfile.txt")
	assert.True(t, exists)

	// get the content of the file
	fileContent, err := client.GetContent(context.Background(), "testbucket", "testfile.txt")
	assert.NoError(t, err)
	assert.Equal(t, content, fileContent)

	defer func() {
		err := minioContainer.Terminate(context.Background())
		assert.NoError(t, err)
	}()
}
