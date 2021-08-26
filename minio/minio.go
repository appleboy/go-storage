package minio

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/appleboy/go-storage/core"

	"github.com/cheggaaa/pb/v3"
	"github.com/h2non/filetype"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
)

var _ core.Storage = (*Minio)(nil)

// Minio client
type Minio struct {
	client *minio.Client
	core   *minio.Core
}

// NewEngine struct
func NewEngine(endpoint, accessID, secretKey string, ssl bool, region string) (*Minio, error) {
	var client *minio.Client
	var core *minio.Core
	var err error
	if endpoint == "" {
		return nil, errors.New("endpoint can't be empty")
	}
	opts := new(minio.Options)
	opts.Region = region
	opts.Secure = ssl
	opts.Creds = credentials.NewStaticV4(accessID, secretKey, "")
	opts.Transport = &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
	}
	// Fetching from IAM roles assigned to an EC2 instance.
	if accessID == "" && secretKey == "" {
		opts.Creds = credentials.NewIAM("")
	}

	client, err = minio.New(endpoint, opts)
	if err != nil {
		return nil, err
	}

	core, err = minio.NewCore(endpoint, opts)
	if err != nil {
		return nil, err
	}

	return &Minio{
		client: client,
		core:   core,
	}, nil
}

// UploadFile to s3 server
func (m *Minio) UploadFile(ctx context.Context, bucketName, objectName string, content []byte, reader io.Reader) error {
	contentType := ""
	kind, _ := filetype.Match(content)
	if kind != filetype.Unknown {
		contentType = kind.MIME.Value
	}

	if contentType == "" {
		contentType = http.DetectContentType(content)
	}

	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	if reader != nil {
		opts.Progress = reader
	}

	// Upload the zip file with FPutObject
	_, err := m.client.PutObject(
		ctx,
		bucketName,
		objectName,
		bytes.NewReader(content),
		int64(len(content)),
		opts,
	)

	return err
}

// UploadFileByReader to s3 server
func (m *Minio) UploadFileByReader(ctx context.Context, bucketName, objectName string, reader io.Reader, contentType string, length int64) error {
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	// Upload the zip file with FPutObject
	_, err := m.client.PutObject(
		ctx,
		bucketName,
		objectName,
		reader,
		length,
		opts,
	)

	return err
}

// CreateBucket create bucket
func (m *Minio) CreateBucket(ctx context.Context, bucketName, region string) error {
	exists, err := m.client.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	return m.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: region})
}

// FilePath for store path + file name
func (m *Minio) FilePath(_, fileName string) string {
	return fmt.Sprintf("%s/%s", os.TempDir(), fileName)
}

// DeleteFile delete file
func (m *Minio) DeleteFile(ctx context.Context, bucketName, fileName string) error {
	return m.client.RemoveObject(ctx, bucketName, fileName, minio.RemoveObjectOptions{})
}

// GetFileURL for storage host + bucket + filename
func (m *Minio) GetFileURL(bucketName, fileName string) string {
	return m.client.EndpointURL().String() + "/" + bucketName + "/" + fileName
}

// DownloadFile downloads and saves the object as a file in the local filesystem.
func (m *Minio) DownloadFile(ctx context.Context, bucketName, fileName, target string) error {
	return m.client.FGetObject(ctx, bucketName, fileName, target, minio.GetObjectOptions{})
}

// DownloadFileByProgress downloads and saves the object as a file in the local filesystem.
func (m *Minio) DownloadFileByProgress(ctx context.Context, bucketName, objectName, filePath string, bar *pb.ProgressBar) error {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return err
	}

	opts := minio.GetObjectOptions{}

	// Verify if destination already exists.
	st, err := os.Stat(filePath)
	if err == nil {
		// If the destination exists and is a directory.
		if st.IsDir() {
			return errInvalidArgument("fileName is a directory.")
		}
	}

	// Proceed if file does not exist. return for all other errors.
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// Extract top level directory.
	objectDir, _ := filepath.Split(filePath)
	if objectDir != "" {
		// Create any missing top level directories.
		if err := os.MkdirAll(objectDir, 0o700); err != nil {
			return err
		}
	}

	// Gather md5sum.
	objectStat, err := m.core.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions(opts))
	if err != nil {
		return err
	}

	// Write to a temporary file "fileName.part.minio" before saving.
	filePartPath := filePath + objectStat.ETag + ".part.minio"

	// If exists, open in append mode. If not create it as a part file.
	filePart, err := os.OpenFile(filePartPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	// If we return early with an error, be sure to close and delete
	// filePart.  If we have an error along the way there is a chance
	// that filePart is somehow damaged, and we should discard it.
	closeAndRemove := true
	defer func() {
		if closeAndRemove {
			_ = filePart.Close()
			_ = os.Remove(filePartPath)
		}
	}()

	// Issue Stat to get the current offset.
	st, err = filePart.Stat()
	if err != nil {
		return err
	}

	// Initialize get object request headers to set the
	// appropriate range offsets to read from.
	if st.Size() > 0 {
		_ = opts.SetRange(st.Size(), 0)
	}

	// Seek to current position for incoming reader.
	objectReader, objectStat, _, err := m.core.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return err
	}

	// progress bar
	bar.SetTotal(objectStat.Size)
	filePartBar := bar.NewProxyWriter(filePart)

	// Write to the part file.
	if _, err = io.CopyN(filePartBar, objectReader, objectStat.Size); err != nil {
		return err
	}

	// Close the file before rename, this is specifically needed for Windows users.
	closeAndRemove = false
	if err = filePart.Close(); err != nil {
		return err
	}

	// Safely completed. Now commit by renaming to actual filename.
	if err = os.Rename(filePartPath, filePath); err != nil {
		return err
	}

	// Return.
	return nil
}

// GetContent for storage bucket + filename
func (m *Minio) GetContent(ctx context.Context, bucketName, fileName string) ([]byte, error) {
	object, err := m.client.GetObject(ctx, bucketName, fileName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(object)

	return buf.Bytes(), nil
}

// CopyFile copy src to dest
func (m *Minio) CopyFile(ctx context.Context, srcBucket, srcPath, destBucket, destPath string) error {
	src := minio.CopySrcOptions{
		Bucket: srcBucket,
		Object: srcPath,
	}
	// Destination object
	dst := minio.CopyDestOptions{
		Bucket: destBucket,
		Object: destPath,
	}
	// Copy object call
	if _, err := m.client.CopyObject(ctx, dst, src); err != nil {
		return err
	}
	return nil
}

// FileExist check object exist. bucket + filename
func (m *Minio) FileExist(ctx context.Context, bucketName, fileName string) bool {
	_, err := m.client.StatObject(ctx, bucketName, fileName, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "AccessDenied" {
			return false
		}
		if errResponse.Code == "NoSuchBucket" {
			return false
		}
		if errResponse.Code == "InvalidBucketName" {
			return false
		}
		if errResponse.Code == "NoSuchKey" {
			return false
		}
		return false
	}

	return true
}

// Client get disk client
func (m *Minio) Client() interface{} {
	return m.client
}

// SignedURL support signed URL
func (m *Minio) SignedURL(ctx context.Context, bucketName, filename string, opts *core.SignedURLOptions) (string, error) {
	// Check if file exists
	if _, err := m.client.StatObject(ctx, bucketName, filename, minio.StatObjectOptions{}); err != nil {
		return "", err
	}

	reqParams := make(url.Values)
	if opts != nil && opts.DefaultFilename != "" {
		reqParams.Set("response-content-disposition", `attachment; filename="`+opts.DefaultFilename+`"`)
	}

	url, err := m.client.PresignedGetObject(ctx, bucketName, filename, opts.Expiry, reqParams)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

// errInvalidArgument - Invalid argument response.
func errInvalidArgument(message string) error {
	return minio.ErrorResponse{
		StatusCode: http.StatusBadRequest,
		Code:       "InvalidArgument",
		Message:    message,
		RequestID:  "minio",
	}
}
