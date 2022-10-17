package handler

import (
	"bytes"
	"fmt"
	"os/exec"
)

func BulkLoad(collectionName string, partitionName string, is_row_based bool, filenames []string) string {
	var bt bytes.Buffer
	for _, filename := range filenames {
		bt.WriteString(filename)
		bt.WriteByte(' ')
	}
	cmd := fmt.Sprintf("python3 bulkload.py %s %s %t %s", collectionName, partitionName, is_row_based, bt.String())
	c := exec.Command("bash", "-c", cmd)
	output, _ := c.CombinedOutput()
	return string(output)
}
