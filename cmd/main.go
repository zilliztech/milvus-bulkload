package main

import (
	"bulkload/internal/handler"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()
	r.MaxMultipartMemory = 8 << 20 // 8 MiB
	r.Static("/", "../public")
	r.POST("/api/v1/bulkload", handler.BulkLoad)
	r.Run(":8080")
}
