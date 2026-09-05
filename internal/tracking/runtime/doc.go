// Package runtime coordinates tracking demand and scanner publication. It
// carries catalog revision and generation fences so work from an old runtime
// cannot publish into the current observation set.
package runtime
