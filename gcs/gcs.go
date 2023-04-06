package gcs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/appleboy/go-storage/core"

	"cloud.google.com/go/storage"
	"github.com/cheggaaa/pb/v3"
	"github.com/h2non/filetype"
)

var _ core.Storage = (*GCS)(nil)

// Google Cloud Storage client
type GCS struct {
	projectID  string
	accessID   string
	privateKey []byte
	client     *storage.Client
}

func downloadFile(ctx context.Context, client *storage.Client, bucketName, fileName, filePath string) error {
	// Verify if destination already exists.
	st, err := os.Stat(filePath)
	if err == nil {
		// If the destination exists and is a directory.
		if st.IsDir() {
			return errors.New("go-storage: fileName is a directory")
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

	obj := client.Bucket(bucketName).Object(fileName)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return err
	}

	// Write to a temporary file "fileName.part.gcs" before saving.
	filePartPath := filePath + attrs.Etag + ".part.gcs"

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

	r, err := obj.NewRangeReader(ctx, st.Size(), attrs.Size)
	if err != nil {
		return err
	}

	// Write to the part file.
	if _, err = io.CopyN(filePart, r, attrs.Size); err != nil {
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
	return nil
}

// NewEngine struct
func NewEngine(projectID string, googleAccessID string, privateKey []byte) (*GCS, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return &GCS{
		client:     client,
		projectID:  projectID,
		accessID:   googleAccessID,
		privateKey: privateKey,
	}, nil
}

// UploadFile to cloud storage
func (g *GCS) UploadFile(ctx context.Context, bucketName, objectName string, content []byte, reader io.Reader) error {
	contentType := ""
	kind, _ := filetype.Match(content)
	if kind != filetype.Unknown {
		contentType = kind.MIME.Value
	}

	if contentType == "" {
		contentType = http.DetectContentType(content)
	}
	w := g.client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	w.ContentType = contentType
	if _, err := io.Copy(w, reader); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

// UploadFileByReader to cloud
func (g *GCS) UploadFileByReader(
	ctx context.Context,
	bucketName, objectName string,
	reader io.Reader, contentType string,
	length int64,
) error {
	w := g.client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	w.ContentType = contentType
	if _, err := io.Copy(w, reader); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

// CreateBucket create bucket
func (g *GCS) CreateBucket(ctx context.Context, bucketName, region string) error {
	return g.client.Bucket(bucketName).Create(ctx, g.projectID, nil)
}

// FilePath for store path + file name
func (g *GCS) FilePath(bucketName, fileName string) string {
	return path.Join("https://storage.googleapis.com", bucketName, fileName)
}

// DeleteFile delete file
func (g *GCS) DeleteFile(ctx context.Context, bucketName, fileName string) error {
	return g.client.Bucket(bucketName).Object(fileName).Delete(ctx)
}

// GetFileURL for storage host + bucket + filename
func (g *GCS) GetFileURL(bucketName, fileName string) string {
	return path.Join("https://storage.googleapis.com", bucketName, fileName)
}

// DownloadFile downloads and saves the object as a file in the local filesystem.
func (g *GCS) DownloadFile(
	ctx context.Context,
	bucketName, objectName, filePath string,
) error {
	return downloadFile(ctx, g.client, bucketName, objectName, filePath)
}

// DownloadFileByProgress downloads and saves the object as a file in the local filesystem.
func (g *GCS) DownloadFileByProgress(
	ctx context.Context,
	bucketName, objectName, filePath string,
	_ *pb.ProgressBar,
) error {
	return downloadFile(ctx, g.client, bucketName, objectName, filePath)
}

// GetContent for storage bucket + filename
func (g *GCS) GetContent(ctx context.Context, bucketName, fileName string) ([]byte, error) {
	buf := new(bytes.Buffer)
	r, err := g.client.Bucket(bucketName).Object(fileName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// CopyFile copy src to dest
func (g *GCS) CopyFile(ctx context.Context, srcBucket, srcPath, destBucket, destPath string) error {
	src := g.client.Bucket(srcBucket).Object(srcPath)
	dst := g.client.Bucket(destBucket).Object(destPath)
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return err
	}
	return nil
}

// FileExist check object exist. bucket + filename
func (g *GCS) FileExist(ctx context.Context, bucketName, fileName string) bool {
	// Check if file exists
	if _, err := g.client.Bucket(bucketName).Object(fileName).Attrs(ctx); err == storage.ErrObjectNotExist {
		return false
	} else if err != nil {
		return false
	}
	return true
}

// BucketExists Checks if a bucket exists.
func (g *GCS) BucketExists(ctx context.Context, bucketName string) (found bool, err error) {
	_, err = g.client.Bucket(bucketName).Attrs(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

// Client get disk client
func (g *GCS) Client() interface{} {
	return g.client
}

// SignedURL support signed URL
func (g *GCS) SignedURL(ctx context.Context, bucketName, fileName string, opts *core.SignedURLOptions) (string, error) {
	// Check if file exists
	if _, err := g.client.Bucket(bucketName).Object(fileName).Attrs(ctx); err != nil {
		return "", err
	}

	return storage.SignedURL(
		bucketName,
		fileName,
		&storage.SignedURLOptions{
			GoogleAccessID: g.accessID,
			PrivateKey:     g.privateKey,
			Method:         "GET",
			Expires:        time.Now().UTC().Add(opts.Expiry),
		})
}
