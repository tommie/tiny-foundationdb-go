package internal

import (
	"fmt"
	"strings"
)

func ByteSliceString(bs []byte) string {
	var sb strings.Builder

	for _, b := range bs {
		if b < 0x20 || b >= 127 {
			fmt.Fprintf(&sb, "\\x%02x", b)
		} else {
			sb.WriteByte(b)
		}
	}
	return sb.String()
}
