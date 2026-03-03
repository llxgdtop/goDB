package dberror

import "fmt"

// Error represents database errors
type Error struct {
	Code    ErrorCode
	Message string
}

type ErrorCode int

const (
	ErrParse ErrorCode = iota
	ErrInternal
	ErrWriteConflict
	ErrTableExists
	ErrTableNotFound
	ErrColumnNotFound
	ErrDuplicateKey
	ErrInvalidValue
	ErrTransactionAborted
)

func (e *Error) Error() string {
	return e.Message
}

// NewParseError creates a parse error
func NewParseError(msg string, args ...interface{}) *Error {
	return &Error{
		Code:    ErrParse,
		Message: fmt.Sprintf(msg, args...),
	}
}

// NewInternalError creates an internal error
func NewInternalError(msg string, args ...interface{}) *Error {
	return &Error{
		Code:    ErrInternal,
		Message: fmt.Sprintf(msg, args...),
	}
}

// NewWriteConflictError creates a write conflict error
func NewWriteConflictError() *Error {
	return &Error{
		Code:    ErrWriteConflict,
		Message: "write conflict detected",
	}
}

// NewTableExistsError creates a table exists error
func NewTableExistsError(name string) *Error {
	return &Error{
		Code:    ErrTableExists,
		Message: fmt.Sprintf("table %s already exists", name),
	}
}

// NewTableNotFoundError creates a table not found error
func NewTableNotFoundError(name string) *Error {
	return &Error{
		Code:    ErrTableNotFound,
		Message: fmt.Sprintf("table %s not found", name),
	}
}

// NewColumnNotFoundError creates a column not found error
func NewColumnNotFoundError(name string) *Error {
	return &Error{
		Code:    ErrColumnNotFound,
		Message: fmt.Sprintf("column %s not found", name),
	}
}

// NewDuplicateKeyError creates a duplicate key error
func NewDuplicateKeyError() *Error {
	return &Error{
		Code:    ErrDuplicateKey,
		Message: "duplicate key",
	}
}

// IsWriteConflict checks if error is a write conflict
func IsWriteConflict(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.Code == ErrWriteConflict
	}
	return false
}
