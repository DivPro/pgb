package pgb

import (
	"strings"
)

type ident string

func (ident ident) Sanitize() string {
	if !strings.ContainsRune(string(ident), '.') {
		s := strings.ReplaceAll(string(ident), string([]byte{0}), "")
		s = strings.ReplaceAll(s, `"`, `""`)

		return `"` + s + `"`
	}

	parts := strings.Split(string(ident), ".")
	for i := range parts {
		s := strings.ReplaceAll(parts[i], string([]byte{0}), "")
		s = strings.ReplaceAll(s, `"`, `""`)
		parts[i] = `"` + s + `"`
	}

	return strings.Join(parts, ".")
}
