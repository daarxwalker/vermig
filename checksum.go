package vermig

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
)

func createChecksum(values ...string) string {
	var buf bytes.Buffer
	for _, value := range values {
		value = strings.ReplaceAll(value, "\r\n", "\n")
		value = strings.TrimSpace(value)
		buf.WriteString(value)
	}
	return fmt.Sprintf("%x", sha256.Sum256(buf.Bytes()))
}
