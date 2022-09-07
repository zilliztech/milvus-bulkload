package handler

import (
	"mime/multipart"
	"net/http"
	"path/filepath"

	"github.com/gin-gonic/gin"
)

type StructA struct {
	CollectionName string `form:"collection_name"`
	PartitionName  string `form:"partition_name"`
	IsRowBased     bool   `form:"is_row_based"`
}

func BulkLoad(c *gin.Context) {
	s := StructA{}
	c.Bind(&s)

	form, err := c.MultipartForm()
	if err != nil {
		c.String(http.StatusBadRequest, "get form err: %s", err.Error())
		return
	}
	files := form.File["files"]

	for _, file := range files {
		filename := filepath.Base(file.Filename)
		ext := filepath.Ext(filename)
		switch ext {
		case "json":
			if err := SaveJsonFile(file); err != nil {
				c.String(http.StatusBadRequest, "upload file err: %s", err.Error())
				return
			}
		case "npy":
			if err := SaveNpyFile(file); err != nil {
				c.String(http.StatusBadRequest, "upload file err: %s", err.Error())
				return
			}
		default:
			c.JSON(500, gin.H{"msg": "Unsupported file: " + filename})
		}
		if err := c.SaveUploadedFile(file, filename); err != nil {
			c.String(http.StatusBadRequest, "upload file err: %s", err.Error())
			return
		}
	}

	c.String(http.StatusOK, "Uploaded successfully %d files, %s", len(files), s)
}

func SaveNpyFile(file *multipart.FileHeader) error {
	return nil
}

func SaveJsonFile(file *multipart.FileHeader) error {
	return nil
}
