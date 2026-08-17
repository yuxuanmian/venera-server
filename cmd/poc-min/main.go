package main

import (
	"fmt"

	"github.com/buke/quickjs-go"
)

func main() {
	runtime := quickjs.NewRuntime()
	ctx := runtime.NewContext()
	ctx.Eval("1+1").Free()
	ctx.Close()
	runtime.Close()
	fmt.Println("MIN OK")
}
