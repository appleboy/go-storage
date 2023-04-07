package core

import (
	"context"
	"io"
	"time"

	"github.com/cheggaaa/pb/v3"
)

// SignedURLOptions download options
type SignedURLOptions struct {
	Expiry          time.Duration
	DefaultFilename string
}

// Storage for s3 and disk
type Storage interface {
	// CreateBucket for create new folder
	CreateBucket(ctx context.Context, bucketName string, region string) error
	// BucketExists Checks if a bucket exists.
	BucketExists(ctx context.Context, bucketName string) (found bool, err error)
	// UploadFile for upload single file
	UploadFile(ctx context.Context, bucketName string, objectName string, content []byte, reader io.Reader) error
	// UploadFileByReader for upload single file
	UploadFileByReader(
		ctx context.Context,
		bucketName string,
		objectName string,
		reader io.Reader,
		contentType string,
		length int64,
	) error
	// DeleteFile for delete single file
	DeleteFile(ctx context.Context, bucketName string, fileName string) error
	// FilePath for store path + file name
	FilePath(bucketName string, fileName string) string
	// GetFile for storage host + bucket + filename
	GetFileURL(bucketName string, fileName string) string
	// DownloadFile downloads and saves the object as a file in the local filesystem.
	DownloadFile(ctx context.Context, bucketName string, objectName string, filePath string) error
	// DownloadFileByProgress downloads and saves the object as a file in the local filesystem.
	DownloadFileByProgress(
		ctx context.Context,
		bucketName string,
		objectName string,
		filePath string,
		bar *pb.ProgressBar,
	) error
	// FileExist check object exist. bucket + filename
	FileExist(ctx context.Context, bucketName string, fileName string) bool
	// GetContent for storage bucket + filename
	GetContent(ctx context.Context, bucketName string, fileName string) ([]byte, error)
	// Copy Create or replace an object through server-side copying of an existing object.
	CopyFile(ctx context.Context, srcBucket string, srcPath string, dstBucket string, dstPath string) error
	// Client get storage client
	Client() interface{}
	// SignedURL get signed URL
	SignedURL(ctx context.Context, bucketName string, filePath string, opts *SignedURLOptions) (string, error)
}
