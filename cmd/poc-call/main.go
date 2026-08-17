package main

import (
	"fmt"
	"os"

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

func main() {
	if len(os.Args) < 6 {
		fmt.Fprintln(os.Stderr, "usage: poc-call <init.js> <script.js> <className> <comicId> <response.json>")
		os.Exit(2)
	}
	initPath := os.Args[1]
	scriptPath := os.Args[2]
	className := os.Args[3]
	comicID := os.Args[4]
	responsePath := os.Args[5]

	initCode, err := os.ReadFile(initPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read init: %v\n", err)
		os.Exit(1)
	}
	scriptCode, err := os.ReadFile(scriptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read script: %v\n", err)
		os.Exit(1)
	}
	responseBody, err := os.ReadFile(responsePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read response: %v\n", err)
		os.Exit(1)
	}

	runtime := quickjs.NewRuntime()
	defer runtime.Close()
	ctx := runtime.NewContext()
	defer ctx.Close()

	response := string(responseBody)
	sendMessage := ctx.NewFunction(func(ctx *quickjs.Context, this *quickjs.Value, args []*quickjs.Value) *quickjs.Value {
		if len(args) == 0 || args[0] == nil {
			return ctx.ThrowError(fmt.Errorf("sendMessage: missing args"))
		}
		methodVal := args[0].Get("method")
		if methodVal == nil {
			return ctx.ThrowError(fmt.Errorf("sendMessage: missing method"))
		}
		method := methodVal.String()
		if method == "http" {
			obj := ctx.NewObject()
			obj.Set("status", ctx.NewInt32(200))
			headers := ctx.NewObject()
			headers.Set("content-type", ctx.NewString("application/json"))
			obj.Set("headers", headers)
			obj.Set("body", ctx.NewString(response))
			return obj
		}
		return ctx.ThrowError(fmt.Errorf("sendMessage: unsupported method %q", method))
	})
	ctx.Globals().Set("sendMessage", sendMessage)

	if err := evalOK(ctx, string(initCode)); err != nil {
		fmt.Printf("INIT FAIL: %v\n", err)
		os.Exit(1)
	}

	wrapper := fmt.Sprintf("(() => { %s\n this['temp'] = new %s()\n })()", string(scriptCode), className)
	if err := evalOK(ctx, wrapper); err != nil {
		fmt.Printf("SCRIPT FAIL: %v\n", err)
		os.Exit(1)
	}

	expr := fmt.Sprintf(`(async () => {
		const s = this['temp'];
		const res = await s.comic.loadInfo(%q);
		return JSON.stringify(res);
	})()`, comicID)
	v := ctx.Eval(expr, quickjs.EvalAwait(true))
	if v == nil {
		fmt.Println("CALL FAIL: eval returned nil")
		os.Exit(1)
	}
	defer v.Free()
	if v.IsException() {
		fmt.Printf("CALL FAIL: %v\n", ctx.Exception())
		os.Exit(1)
	}
	fmt.Println(v.String())
}
