package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/buke/quickjs-go"
)

func evalOK(ctx *quickjs.Context, code string) error {
	v := ctx.Eval(code)
	if v == nil {
		return fmt.Errorf("eval returned nil")
	}
	defer v.Free()
	if v.IsException() {
		return ctx.Exception()
	}
	return nil
}

func checkScript(initCode string, scriptPath string) error {
	runtime := quickjs.NewRuntime()
	defer runtime.Close()
	ctx := runtime.NewContext()
	defer ctx.Close()

	sendMessage := ctx.NewFunction(func(ctx *quickjs.Context, this *quickjs.Value, args []*quickjs.Value) *quickjs.Value {
		return ctx.ThrowError(fmt.Errorf("sendMessage called during load"))
	})
	ctx.Globals().Set("sendMessage", sendMessage)

	if err := evalOK(ctx, initCode); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	code, err := os.ReadFile(scriptPath)
	if err != nil {
		return err
	}
	if err := evalOK(ctx, string(code)); err != nil {
		return err
	}
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: poc-smoke <init.js> <script.js> [more scripts...]")
		os.Exit(2)
	}
	initPath := os.Args[1]
	scriptPaths := os.Args[2:]

	initCode, err := os.ReadFile(initPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read init.js: %v\n", err)
		os.Exit(1)
	}

	failed := false
	for _, p := range scriptPaths {
		err := checkScript(string(initCode), p)
		if err != nil {
			fmt.Printf("SCRIPT FAIL %s: %v\n", filepath.Base(p), err)
			failed = true
		} else {
			fmt.Printf("SCRIPT OK %s\n", filepath.Base(p))
		}
	}

	if failed {
		fmt.Println("SMOKE FAIL")
		os.Exit(1)
	}
	fmt.Println("SMOKE PASS")
}
