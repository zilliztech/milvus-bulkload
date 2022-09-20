package handler

import (
	"bulkload/internal/config"
	"bulkload/internal/result"
	"bulkload/internal/util"
	"bytes"
	"io"
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
		case ".csv":
			if err := SaveCsvFile(file, filenames[i], opt.IsRowBased); err != nil {
				c.JSON(500, result.Err.WithMsg("upload file err: "+err.Error()))
				return
			}
		default:
			c.JSON(500, result.Err.WithMsg("Unsupported file: "+filenames[i]))
		}
	}
	// call api do bulkload
	msg := BulkLoad(opt.CollectionName, opt.PartitionName, opt.IsRowBased, filenames)

	c.JSON(200, result.OK.WithMsg(msg))
}

func SaveJsonFile(file *multipart.FileHeader, filename string, isRowBased bool) error {
	// 把文件保存到minio
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	return SaveToMinio(src, config.BUCKET_NAME, filename)
}

func SaveNpyFile(file *multipart.FileHeader, filename string, isRowBased bool) error {
	// save npy file to minio
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	return SaveToMinio(src, config.BUCKET_NAME, filename)
}

func SaveCsvFile(file *multipart.FileHeader, filename string, isRowBased bool) error {
	// save npy file to minio
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	csvData, _ := io.ReadAll(src)
	jsonData := util.Csv2Json(csvData)
	return SaveToMinio(bytes.NewReader(jsonData), config.BUCKET_NAME, filename)
}
