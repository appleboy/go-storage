package storage

import (
	"github.com/appleboy/go-storage/core"
	"github.com/appleboy/go-storage/disk"
	"github.com/appleboy/go-storage/minio"
)

// S3 for storage interface
var S3 core.Storage

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
func NewEngine(cfg Config) (core.Storage, error) {
	var err error
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
			return nil, err
		}
	case "disk":
		S3 = disk.NewEngine(
			cfg.Addr,
			cfg.Path,
		)
	}

	return S3, nil
}

// NewS3Engine return storage interface
func NewS3Engine(endPoint, accessID, secretKey string, ssl bool, region string) (core.Storage, error) {
	return minio.NewEngine(
		endPoint,
		accessID,
		secretKey,
		ssl,
		region,
	)
}

// NewDiskEngine return storage interface
func NewDiskEngine(host, folder string) (core.Storage, error) {
	return disk.NewEngine(
		host,
		folder,
	), nil
}
