package handler

import (
	"bulkload/internal/config"
	"context"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var instance *minio.Client

func GetClient() *minio.Client {
	return instance
}

func init() {
	instance, _ = minio.New(config.MINIO_ENDPOINT, &minio.Options{
		Creds: credentials.NewStaticV4(config.MINIO_SECRET_KEY, config.MINIO_ACCESS_KEY, ""),
	})
}

func SaveToMinio(src io.Reader, bucketName string, objectName string) error {
	client := GetClient()
	_, err := client.PutObject(context.Background(), bucketName, objectName, src, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}
