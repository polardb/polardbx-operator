package errors

// ErrorResponse is the standard JSON error envelope returned by the API.
// It matches APIError.ToResponse().
//
// Example:
// {
//   "error": {
//     "code": "VAL_3001",
//     "message": "Validation failed",
//     "details": { ... }
//   }
// }
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody is the payload under the "error" field.
type ErrorBody struct {
	Code    ErrorCode    `json:"code"`
	Message string       `json:"message"`
	Details interface{}  `json:"details,omitempty"`
}

// MessageResponse is a common success envelope used by many endpoints.
type MessageResponse struct {
	Message string `json:"message"`
}

