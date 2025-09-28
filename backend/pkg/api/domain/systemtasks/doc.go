package systemtasks

// Package systemtasks provides the unified entrypoint for the platform system tasks domain.
//
// Domain boundaries (BFF):
// - SystemTask CRUD and status management
//
// Phase 1: thin route wrappers forwarding to existing handlers; interfaces unchanged.
// Related CRD (api/v1): SystemTask.
