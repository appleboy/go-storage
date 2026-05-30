package storage

import (
	"fmt"

	"github.com/appleboy/go-storage/core"
	"github.com/appleboy/go-storage/disk"
	"github.com/appleboy/go-storage/minio"
)

// S3 for storage interface
var S3 core.Storage

// Config for storage
type Config struct {
	Endpoint           string
	AccessID           string
	SecretKey          string
	SSL                bool
	InsecureSkipVerify bool
	Region             string
	Path               string
	Bucket             string
	Addr               string
	Driver             string
}

// NewEngine return storage interface
func NewEngine(cfg Config) (core.Storage, error) {
	switch cfg.Driver {
	case "s3":
		engine, err := minio.NewEngine(
			cfg.Endpoint,
			cfg.AccessID,
			cfg.SecretKey,
			cfg.SSL,
			cfg.InsecureSkipVerify,
			cfg.Region,
		)
		if err != nil {
			return nil, err
		}
		S3 = engine
		return engine, nil
	case "disk":
		engine := disk.NewEngine(
			cfg.Addr,
			cfg.Path,
		)
		S3 = engine
		return engine, nil
	default:
		return nil, fmt.Errorf("unknown storage driver: %q", cfg.Driver)
	}
}

// NewS3Engine return storage interface
func NewS3Engine(
	endPoint, accessID, secretKey string,
	ssl, insecureSkipVerify bool,
	region string,
) (core.Storage, error) {
	engine, err := minio.NewEngine(
		endPoint,
		accessID,
		secretKey,
		ssl,
		insecureSkipVerify,
		region,
	)
	if err != nil {
		return nil, err
	}
	return engine, nil
}

// NewDiskEngine return storage interface
func NewDiskEngine(host, folder string) (core.Storage, error) {
	return disk.NewEngine(
		host,
		folder,
	), nil
}
