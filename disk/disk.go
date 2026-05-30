package disk

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/appleboy/go-storage/core"

	"github.com/cheggaaa/pb/v3"
)

var _ core.Storage = (*Disk)(nil)

func copyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("file %s already exists", dst)
	}

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}

	if _, err := io.Copy(destination, source); err != nil {
		// Discard the half-written destination so a retry is not blocked
		// by the "already exists" guard above and readers never see a
		// truncated file.
		_ = destination.Close()
		_ = os.Remove(dst)
		return err
	}
	return destination.Close()
}

// Disk client
type Disk struct {
	Host string
	Path string
}

// NewEngine struct
func NewEngine(host, path string) *Disk {
	return &Disk{
		Host: host,
		Path: path,
	}
}

// UploadFile to upload file to disk
func (d *Disk) UploadFile(
	_ context.Context,
	bucketName, fileName string,
	content []byte,
	_ io.Reader,
) error {
	// check folder exists
	// ex: bucket + foo/bar/uuid.tar.gz
	storage := path.Join(d.Path, bucketName, filepath.Dir(fileName))
	if err := os.MkdirAll(storage, os.ModePerm); err != nil {
		return err
	}
	return os.WriteFile(d.FilePath(bucketName, fileName), content, os.FileMode(0o644))
}

// UploadFileByReader to upload file to disk
func (d *Disk) UploadFileByReader(
	_ context.Context,
	bucketName, fileName string,
	reader io.Reader,
	_ string, _ int64,
) error {
	// check folder exists
	// ex: bucket + foo/bar/uuid.tar.gz
	storage := path.Join(d.Path, bucketName, filepath.Dir(fileName))
	if err := os.MkdirAll(storage, os.ModePerm); err != nil {
		return err
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return os.WriteFile(d.FilePath(bucketName, fileName), content, os.FileMode(0o644))
}

// CreateBucket create bucket
func (d *Disk) CreateBucket(_ context.Context, bucketName, region string) error {
	storage := path.Join(d.Path, bucketName)
	if err := os.MkdirAll(storage, os.ModePerm); err != nil {
		return err
	}

	return nil
}

// FilePath for store path + file name
func (d *Disk) FilePath(bucketName, fileName string) string {
	return path.Join(
		d.Path,
		bucketName,
		fileName,
	)
}

// DeleteFile delete file
func (d *Disk) DeleteFile(_ context.Context, bucketName, fileName string) error {
	return os.Remove(d.FilePath(bucketName, fileName))
}

// GetFileURL for storage host + bucket + filename
func (d *Disk) GetFileURL(bucketName, fileName string) string {
	if d.Host != "" {
		if u, err := url.Parse(d.Host); err == nil {
			u.Path = path.Join(u.Path, d.Path, bucketName, fileName)
			return u.String()
		}
	}
	return path.Join(d.Path, bucketName, fileName)
}

// DownloadFile downloads and saves the object as a file in the local filesystem.
func (d *Disk) DownloadFile(_ context.Context, bucketName, fileName, target string) error {
	return nil
}

// DownloadFileByProgress downloads and saves the object as a file in the local filesystem.
func (d *Disk) DownloadFileByProgress(
	_ context.Context,
	bucketName, fileName, target string,
	_ *pb.ProgressBar,
) error {
	return nil
}

// GetContent for storage bucket + filename
func (d *Disk) GetContent(_ context.Context, bucketName, fileName string) ([]byte, error) {
	return os.ReadFile(d.FilePath(bucketName, fileName))
}

// CopyFile copy src to dest
func (d *Disk) CopyFile(
	_ context.Context,
	srcBucketName, srcFile, destBucketName, destFile string,
) error {
	src := d.FilePath(srcBucketName, srcFile)
	dest := d.FilePath(destBucketName, destFile)
	return copyFile(src, dest)
}

// FileExist check object exist. bucket + filename
func (d *Disk) FileExist(_ context.Context, bucketName, fileName string) bool {
	_, err := os.Stat(d.FilePath(bucketName, fileName))

	return err == nil
}

// BucketExists Checks if a bucket exists.
func (d *Disk) BucketExists(_ context.Context, bucketName string) (found bool, err error) {
	if _, err := os.Stat(d.FilePath(bucketName, "")); err != nil {
		// A missing bucket directory is the normal "does not exist" case,
		// not a failure; only surface real stat errors.
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Client get disk client
func (d *Disk) Client() interface{} {
	return nil
}

// SignedURL support signed URL
func (d *Disk) SignedURL(
	_ context.Context,
	bucketName, filename string,
	opts *core.SignedURLOptions,
) (string, error) {
	return d.GetFileURL(bucketName, filename), nil
}

func (d *Disk) SetLifeCycle(_ context.Context, _ string, _ *core.LifecycleConfig) error {
	return nil
}
