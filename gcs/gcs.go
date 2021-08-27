package gcs

import (
	"context"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"github.com/appleboy/go-storage/core"

	"github.com/cheggaaa/pb/v3"
)

var _ core.Storage = (*GCS)(nil)

// Google Cloud Storage client
type GCS struct {
	client *storage.Client
}

// NewEngine struct
func NewEngine() (*GCS, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return &GCS{
		client: client,
	}, nil
}

// UploadFile to cloud storage
func (g *GCS) UploadFile(ctx context.Context, bucketName, objectName string, content []byte, reader io.Reader) error {
	// contentType := ""
	// kind, _ := filetype.Match(content)
	// if kind != filetype.Unknown {
	// 	contentType = kind.MIME.Value
	// }

	// if contentType == "" {
	// 	contentType = http.DetectContentType(content)
	// }

	// opts := minio.PutObjectOptions{
	// 	ContentType: contentType,
	// }
	// if reader != nil {
	// 	opts.Progress = reader
	// }

	// // Upload the zip file with FPutObject
	// _, err := m.client.PutObject(
	// 	ctx,
	// 	bucketName,
	// 	objectName,
	// 	bytes.NewReader(content),
	// 	int64(len(content)),
	// 	opts,
	// )

	return nil
}

// UploadFileByReader to s3 server
func (g *GCS) UploadFileByReader(ctx context.Context, bucketName, objectName string, reader io.Reader, contentType string, length int64) error {
	// opts := minio.PutObjectOptions{
	// 	ContentType: contentType,
	// }
	// // Upload the zip file with FPutObject
	// _, err := m.client.PutObject(
	// 	ctx,
	// 	bucketName,
	// 	objectName,
	// 	reader,
	// 	length,
	// 	opts,
	// )

	// return err
	return nil
}

// CreateBucket create bucket
func (g *GCS) CreateBucket(ctx context.Context, bucketName, region string) error {
	// exists, err := m.client.BucketExists(ctx, bucketName)
	// if err != nil {
	// 	return err
	// }

	// if exists {
	// 	return nil
	// }

	// return m.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: region})
	return nil
}

// FilePath for store path + file name
func (g *GCS) FilePath(_, fileName string) string {
	return fmt.Sprintf("%s/%s", os.TempDir(), fileName)
}

// DeleteFile delete file
func (g *GCS) DeleteFile(ctx context.Context, bucketName, fileName string) error {
	// return m.client.RemoveObject(ctx, bucketName, fileName, minio.RemoveObjectOptions{})
	return nil
}

// GetFileURL for storage host + bucket + filename
func (g *GCS) GetFileURL(bucketName, fileName string) string {
	// return m.client.EndpointURL().String() + "/" + bucketName + "/" + fileName
	return ""
}

// DownloadFile downloads and saves the object as a file in the local filesystem.
func (g *GCS) DownloadFile(ctx context.Context, bucketName, fileName, target string) error {
	// return m.client.FGetObject(ctx, bucketName, fileName, target, minio.GetObjectOptions{})
	return nil
}

// DownloadFileByProgress downloads and saves the object as a file in the local filesystem.
func (g *GCS) DownloadFileByProgress(ctx context.Context, bucketName, objectName, filePath string, bar *pb.ProgressBar) error {
	// Return.
	return nil
}

// GetContent for storage bucket + filename
func (g *GCS) GetContent(ctx context.Context, bucketName, fileName string) ([]byte, error) {
	// object, err := m.client.GetObject(ctx, bucketName, fileName, minio.GetObjectOptions{})
	// if err != nil {
	// 	return nil, err
	// }

	// buf := new(bytes.Buffer)
	// if _, err := buf.ReadFrom(object); err != nil {
	// 	return nil, err
	// }

	// return buf.Bytes(), nil
	return nil, nil
}

// CopyFile copy src to dest
func (g *GCS) CopyFile(ctx context.Context, srcBucket, srcPath, destBucket, destPath string) error {
	// src := minio.CopySrcOptions{
	// 	Bucket: srcBucket,
	// 	Object: srcPath,
	// }
	// // Destination object
	// dst := minio.CopyDestOptions{
	// 	Bucket: destBucket,
	// 	Object: destPath,
	// }
	// // Copy object call
	// if _, err := m.client.CopyObject(ctx, dst, src); err != nil {
	// 	return err
	// }
	return nil
}

// FileExist check object exist. bucket + filename
func (g *GCS) FileExist(ctx context.Context, bucketName, fileName string) bool {
	// _, err := m.client.StatObject(ctx, bucketName, fileName, minio.StatObjectOptions{})
	// if err != nil {
	// 	errResponse := minio.ToErrorResponse(err)
	// 	if errResponse.Code == "AccessDenied" {
	// 		return false
	// 	}
	// 	if errResponse.Code == "NoSuchBucket" {
	// 		return false
	// 	}
	// 	if errResponse.Code == "InvalidBucketName" {
	// 		return false
	// 	}
	// 	if errResponse.Code == "NoSuchKey" {
	// 		return false
	// 	}
	// 	return false
	// }

	return true
}

// Client get disk client
func (g *GCS) Client() interface{} {
	return g.client
}

// SignedURL support signed URL
func (g *GCS) SignedURL(ctx context.Context, bucketName, filename string, opts *core.SignedURLOptions) (string, error) {
	// Check if file exists
	// if _, err := m.client.StatObject(ctx, bucketName, filename, minio.StatObjectOptions{}); err != nil {
	// 	return "", err
	// }

	// reqParams := make(url.Values)
	// if opts != nil && opts.DefaultFilename != "" {
	// 	reqParams.Set("response-content-disposition", `attachment; filename="`+opts.DefaultFilename+`"`)
	// }

	// url, err := m.client.PresignedGetObject(ctx, bucketName, filename, opts.Expiry, reqParams)
	// if err != nil {
	// 	return "", err
	// }

	return "", nil
}
