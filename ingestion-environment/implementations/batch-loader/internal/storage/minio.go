package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	mc     *minio.Client
	bucket string
}

func NewMinIO(endpoint, access, secret string, useTLS bool, bucket string) (*Client, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: useTLS,
	})
	if err != nil {
		return nil, err
	}
	return &Client{mc: mc, bucket: bucket}, nil
}

func (c *Client) EnsureBucket(ctx context.Context) error {
	exists, err := c.mc.BucketExists(ctx, c.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return c.mc.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{})
	}
	return nil
}

func (c *Client) Upload(ctx context.Context, objectName string, r io.Reader, size int64, contentType string) error {
	_, err := c.mc.PutObject(ctx, c.bucket, objectName, r, size, minio.PutObjectOptions{
		ContentType:      contentType,
		DisableMultipart: false,
	})
	return err
}

func BuildObjectPath(basePath string, t time.Time, file string) string {
	return fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/%s",
		basePath, t.UTC().Year(), t.UTC().Month(), t.UTC().Day(), file)
}
