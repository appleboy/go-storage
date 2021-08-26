package storage

import (
	"context"
	"io"

	"github.com/appleboy/go-storage/core"
	"github.com/appleboy/go-storage/disk"
	"github.com/appleboy/go-storage/minio"

	"github.com/cheggaaa/pb/v3"
)

// Storage for s3 and disk
type Storage interface {
	// CreateBucket for create new folder
	CreateBucket(context.Context, string, string) error
	// UploadFile for upload single file
	UploadFile(context.Context, string, string, []byte, io.Reader) error
	// UploadFileByReader for upload single file
	UploadFileByReader(context.Context, string, string, io.Reader, string, int64) error
	// DeleteFile for delete single file
	DeleteFile(context.Context, string, string) error
	// FilePath for store path + file name
	FilePath(string, string) string
	// GetFile for storage host + bucket + filename
	GetFileURL(string, string) string
	// DownloadFile downloads and saves the object as a file in the local filesystem.
	DownloadFile(context.Context, string, string, string) error
	// DownloadFileByProgress downloads and saves the object as a file in the local filesystem.
	DownloadFileByProgress(context.Context, string, string, string, *pb.ProgressBar) error
	// FileExist check object exist. bucket + filename
	FileExist(context.Context, string, string) bool
	// GetContent for storage bucket + filename
	GetContent(context.Context, string, string) ([]byte, error)
	// Copy Create or replace an object through server-side copying of an existing object.
	CopyFile(context.Context, string, string, string, string) error
	// Client get storage client
	Client() interface{}
	// SignedURL get signed URL
	SignedURL(context.Context, string, string, *core.SignedURLOptions) (string, error)
}

// S3 for storage interface
var S3 Storage

// Config for storage
type Config struct {
	Endpoint  string
	AccessID  string
	SecretKey string
	SSL       bool
	Region    string
	Path      string
	Bucket    string
	Addr      string
	Driver    string
}

// NewEngine return storage interface
func NewEngine(cfg Config) (err error) {
	switch cfg.Driver {
	case "s3":
		S3, err = minio.NewEngine(
			cfg.Endpoint,
			cfg.AccessID,
			cfg.SecretKey,
			cfg.SSL,
			cfg.Region,
		)
		if err != nil {
			return err
		}
	case "disk":
		S3 = disk.NewEngine(
			cfg.Addr,
			cfg.Path,
		)
	}
	ctx := context.Background()
	return S3.CreateBucket(ctx, cfg.Bucket, cfg.Region)
}

// NewS3Engine return storage interface
func NewS3Engine(endPoint, accessID, secretKey string, ssl bool, region string) (Storage, error) {
	return minio.NewEngine(
		endPoint,
		accessID,
		secretKey,
		ssl,
		region,
	)
}

// NewDiskEngine return storage interface
func NewDiskEngine(host, folder string) (Storage, error) {
	return disk.NewEngine(
		host,
		folder,
	), nil
}
