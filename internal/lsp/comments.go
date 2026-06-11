package lsp

import (
	"strings"
)

// CleanBlockComment removes the opening '/*' and closing '*/' decorators from a Heddle block comment,
// along with any leading/trailing empty lines and common block asterisks, returning a cleaned technical description.
func CleanBlockComment(comment string) string {
	comment = strings.TrimSpace(comment)
	comment = strings.TrimPrefix(comment, "/*")
	comment = strings.TrimSuffix(comment, "*/")

	lines := strings.Split(comment, "\n")
	var cleanedLines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "*") {
			line = strings.TrimSpace(line[1:])
		}
		cleanedLines = append(cleanedLines, line)
	}

	// Trim empty leading/trailing lines
	start := 0
	for start < len(cleanedLines) && cleanedLines[start] == "" {
		start++
	}
	end := len(cleanedLines)
	for end > start && cleanedLines[end-1] == "" {
		end--
	}

	if start >= end {
		return ""
	}

	return strings.Join(cleanedLines[start:end], "\n")
}
