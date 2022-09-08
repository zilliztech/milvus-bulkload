package handler

import (
	"bulkload/internal/config"
	"bulkload/internal/result"
	"mime/multipart"
	"path/filepath"

	"github.com/gin-gonic/gin"
)

type BulkLoadOpt struct {
	CollectionName string `form:"collection_name"`
	PartitionName  string `form:"partition_name"`
	IsRowBased     bool   `form:"is_row_based"`
}

func HandleRequest(c *gin.Context) {
	opt := BulkLoadOpt{}
	c.Bind(&opt)
	form, err := c.MultipartForm()
	if err != nil {
		c.JSON(500, result.Err.WithMsg(err.Error()))
		return
	}
	files := form.File["files"]
	// 将上传的文件转换为npy文件保存到minio
	filenames := make([]string, len(files))
	for i, file := range files {
		filenames[i] = filepath.Base(file.Filename)
		ext := filepath.Ext(filenames[i])
		switch ext {
		case ".json":
			if err := SaveJsonFile(file, filenames[i], opt.IsRowBased); err != nil {
				c.JSON(500, result.Err.WithMsg("upload file err: "+err.Error()))
				return
			}
		case ".npy":
			if err := SaveNpyFile(file, filenames[i], opt.IsRowBased); err != nil {
				c.JSON(500, result.Err.WithMsg("upload file err: "+err.Error()))
				return
			}
		default:
			c.JSON(500, result.Err.WithMsg("Unsupported file: "+filenames[i]))
		}
	}
	// 调用API执行bulkload
	msg := BulkLoad(opt.CollectionName, opt.PartitionName, opt.IsRowBased, filenames)

	c.JSON(200, result.OK.WithMsg(msg))
}

func SaveJsonFile(file *multipart.FileHeader, filename string, isRowBased bool) error {
	// 将json文件转换为npy文件
	// 把文件保存到minio
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	return SaveToMinio(src, config.BUCKET_NAME, filename)
}

func SaveNpyFile(file *multipart.FileHeader, filename string, isRowBased bool) error {
	// 将文件保存到minio
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	return SaveToMinio(src, config.BUCKET_NAME, filename)
}
