package precompute

// This file previously contained memory-optimized approaches for finding valid codes.
// After performance testing, the SQLite-based lowmem mode (sqlite.go) proved to be
// both faster (~10 min vs 81 min) and more memory efficient (<1GB vs 2-3GB).
//
// All code intersection logic now uses the lowmem SQLite approach in sqlite.go.
