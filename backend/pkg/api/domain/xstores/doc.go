package xstores

// Package xstores provides the unified entrypoint for the storage/engine domain.
//
// Domain boundaries (BFF):
// - XStore CRUD and Pods
// - Backups (XStoreBackup) and related helpers
// - Backup binlogs (XStoreBackupBinlog) for Standard Edition
// - Followers management and rebuild flows (logger/learner/auto)
//
// Phase 1 (low risk): provide thin route wrappers forwarding to existing handlers;
// keep paths and request/response formats unchanged.
