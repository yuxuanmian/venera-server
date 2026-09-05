// Package store owns additive SQLite persistence for tracking catalog state,
// client state, interests, and current observations. It keeps transactions
// and database invariants below the API and runtime layers.
package store
