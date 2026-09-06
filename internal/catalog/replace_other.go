//go:build !windows

package catalog

import "os"

func replaceFile(source, target string) error { return os.Rename(source, target) }
