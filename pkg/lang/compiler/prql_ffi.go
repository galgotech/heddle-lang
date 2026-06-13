package compiler

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../internal/datafusion-ffi/target/release -ldatafusion_ffi -ldl -lm -lpthread
#include <stdlib.h>

extern char* validate_prql(const char* query, char** error_out);
extern void free_string(char* ptr);
*/
import "C"

import (
	"errors"
	"strings"
	"unsafe"
)

// ParsePRQLTables compiles the PRQL query and returns:
// 1. A list of external tables referenced (must exist as assignments).
// 2. A list of table aliases defined (must NOT conflict with assignments).
// Or an error if the PRQL query syntax is invalid.
func ParsePRQLTables(query string) ([]string, []string, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var cError *C.char
	cResult := C.validate_prql(cQuery, &cError)
	if cError != nil {
		errStr := C.GoString(cError)
		C.free_string(cError)
		return nil, nil, errors.New(errStr)
	}

	if cResult == nil {
		return nil, nil, nil
	}

	resultStr := C.GoString(cResult)
	C.free_string(cResult)

	if resultStr == "" {
		return nil, nil, nil
	}

	parts := strings.Split(resultStr, ";")
	if len(parts) != 2 {
		return nil, nil, nil
	}

	var tables []string
	for _, p := range strings.Split(parts[0], ",") {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			tables = append(tables, trimmed)
		}
	}

	var aliases []string
	for _, p := range strings.Split(parts[1], ",") {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			aliases = append(aliases, trimmed)
		}
	}

	return tables, aliases, nil
}

// cleanPRQL strips outer parenthesis and trims whitespace from a PRQL query string.
func cleanPRQL(query string) string {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(query, "(") && strings.HasSuffix(query, ")") {
		query = query[1 : len(query)-1]
	}
	return strings.TrimSpace(query)
}
