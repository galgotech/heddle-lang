package core

import "fmt"

// BusinessError represents an expected logic validation failure.
// When returned by user code, it signals to the Control Plane
// that a specific, structured business error occurred rather than a fatal crash.
type BusinessError struct {
	Message string
	Code    int // Optional business logic code
}

func (e *BusinessError) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("BusinessError [%d]: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("BusinessError: %s", e.Message)
}

// NewBusinessError creates a new BusinessError.
func NewBusinessError(message string) error {
	return &BusinessError{Message: message}
}

// NewBusinessErrorCode creates a new BusinessError with a specific code.
func NewBusinessErrorCode(code int, message string) error {
	return &BusinessError{Code: code, Message: message}
}
