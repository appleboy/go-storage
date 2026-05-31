package disk

import (
	"context"
	"testing"
)

func TestDisk_BucketExists(t *testing.T) {
	d := NewEngine("", t.TempDir())

	// A missing bucket must report (false, nil), not an error.
	found, err := d.BucketExists(context.Background(), "missing")
	if err != nil {
		t.Fatalf("BucketExists(missing) returned error: %v", err)
	}
	if found {
		t.Errorf("BucketExists(missing) = true, want false")
	}

	// After creation it must report (true, nil).
	if err := d.CreateBucket(context.Background(), "present", ""); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	found, err = d.BucketExists(context.Background(), "present")
	if err != nil {
		t.Fatalf("BucketExists(present) returned error: %v", err)
	}
	if !found {
		t.Errorf("BucketExists(present) = false, want true")
	}
}

func TestDisk_GetFileURL(t *testing.T) {
	type fields struct {
		Host string
		Path string
	}
	type args struct {
		bucketName string
		fileName   string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "without host",
			fields: fields{
				Path: "./data/",
			},
			args: args{
				bucketName: "test",
				fileName:   "a.png",
			},
			want: "data/test/a.png",
		},
		{
			name: "without host and absolute path",
			fields: fields{
				Path: "/data/",
			},
			args: args{
				bucketName: "test",
				fileName:   "a.png",
			},
			want: "/data/test/a.png",
		},
		{
			name: "with host",
			fields: fields{
				Host: "http://localhost:8080/",
				Path: "./data/",
			},
			args: args{
				bucketName: "test",
				fileName:   "a.png",
			},
			want: "http://localhost:8080/data/test/a.png",
		},
		{
			name: "with host and absolute path",
			fields: fields{
				Host: "http://localhost:8080/",
				Path: "/data/",
			},
			args: args{
				bucketName: "test",
				fileName:   "a.png",
			},
			want: "http://localhost:8080/data/test/a.png",
		},
		{
			name: "wrong host format",
			fields: fields{
				Host: "localhost",
				Path: "/data/",
			},
			args: args{
				bucketName: "test",
				fileName:   "a.png",
			},
			want: "localhost/data/test/a.png",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Disk{
				Host: tt.fields.Host,
				Path: tt.fields.Path,
			}
			if got := d.GetFileURL(tt.args.bucketName, tt.args.fileName); got != tt.want {
				t.Errorf("Disk.GetFileURL() = %v, want %v", got, tt.want)
			}
		})
	}
}
