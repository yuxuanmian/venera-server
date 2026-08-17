package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/buke/quickjs-go"
)

type RecordedRequest struct {
	TS               string            `json:"ts"`
	Method           string            `json:"method"`
	URL              string            `json:"url"`
	RequestHeaders   map[string]string `json:"request_headers,omitempty"`
	RequestBody      string            `json:"request_body,omitempty"`
	ResponseStatus   int               `json:"response_status"`
	ResponseHeaders  map[string]string `json:"response_headers,omitempty"`
	ResponseBodyBase string            `json:"response_body_base64,omitempty"`
	ResponseBodyText string            `json:"response_body_text,omitempty"`
}

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

func headersFromJS(ctx *quickjs.Context, v *quickjs.Value) map[string]string {
	out := map[string]string{}
	if v == nil || !v.IsObject() {
		return out
	}
	names, err := v.PropertyNames()
	if err != nil {
		return out
	}
	for _, name := range names {
		val := v.Get(name)
		if val != nil {
			out[name] = val.String()
		}
	}
	return out
}

func headersToJS(ctx *quickjs.Context, h http.Header) *quickjs.Value {
	obj := ctx.NewObject()
	for k, vs := range h {
		if len(vs) > 0 {
			obj.Set(k, ctx.NewString(vs[0]))
		}
	}
	return obj
}

func main() {
	if len(os.Args) < 6 {
		fmt.Fprintln(os.Stderr, "usage: poc-real <init.js> <script.js> <className> <comicId> <output.jsonl> [cookieHeader]")
		os.Exit(2)
	}
	initPath := os.Args[1]
	scriptPath := os.Args[2]
	className := os.Args[3]
	comicID := os.Args[4]
	outputPath := os.Args[5]
	cookieHeader := ""
	if len(os.Args) >= 7 {
		cookieHeader = os.Args[6]
	}

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

	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open output: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	client := &http.Client{Timeout: 30 * time.Second}

	runtime := quickjs.NewRuntime()
	defer runtime.Close()
	ctx := runtime.NewContext()
	defer ctx.Close()

	sendMessage := ctx.NewFunction(func(ctx *quickjs.Context, this *quickjs.Value, args []*quickjs.Value) *quickjs.Value {
		if len(args) == 0 || args[0] == nil {
			return ctx.ThrowError(fmt.Errorf("sendMessage: missing args"))
		}
		methodVal := args[0].Get("method")
		if methodVal == nil {
			return ctx.ThrowError(fmt.Errorf("sendMessage: missing method"))
		}
		method := methodVal.String()

		switch method {
		case "http":
			httpMethod := args[0].Get("http_method").String()
			url := args[0].Get("url").String()
			headers := headersFromJS(ctx, args[0].Get("headers"))
			if cookieHeader != "" {
				if _, ok := headers["Cookie"]; !ok {
					headers["Cookie"] = cookieHeader
				}
			}
			dataVal := args[0].Get("data")
			var body io.Reader
			bodyStr := ""
			if dataVal != nil && !dataVal.IsNull() && !dataVal.IsUndefined() {
				bodyStr = dataVal.String()
				body = strings.NewReader(bodyStr)
			}

			req, err := http.NewRequest(httpMethod, url, body)
			if err != nil {
				return ctx.ThrowError(fmt.Errorf("new request: %w", err))
			}
			for k, v := range headers {
				req.Header.Set(k, v)
			}

			resp, err := client.Do(req)
			if err != nil {
				return ctx.ThrowError(fmt.Errorf("http do: %w", err))
			}
			defer resp.Body.Close()
			respBody, err := io.ReadAll(io.LimitReader(resp.Body, 20<<20))
			if err != nil {
				return ctx.ThrowError(fmt.Errorf("read body: %w", err))
			}

			rec := RecordedRequest{
				TS:              time.Now().UTC().Format(time.RFC3339),
				Method:          httpMethod,
				URL:             url,
				RequestHeaders:  headers,
				RequestBody:     bodyStr,
				ResponseStatus:  resp.StatusCode,
				ResponseHeaders: map[string]string{},
			}
			for k, vs := range resp.Header {
				if len(vs) > 0 {
					rec.ResponseHeaders[k] = vs[0]
				}
			}
			// 优先以文本保存，若含不可打印字符则用 base64。
			if isPrintable(respBody) {
				rec.ResponseBodyText = string(respBody)
			} else {
				rec.ResponseBodyBase = base64.StdEncoding.EncodeToString(respBody)
			}
			line, _ := json.Marshal(rec)
			fmt.Fprintln(outFile, string(line))

			obj := ctx.NewObject()
			obj.Set("status", ctx.NewInt32(int32(resp.StatusCode)))
			obj.Set("headers", headersToJS(ctx, resp.Header))
			bytesFlag := args[0].Get("bytes")
			if bytesFlag != nil && bytesFlag.Bool() {
				obj.Set("body", ctx.NewArrayBuffer(respBody))
			} else {
				obj.Set("body", ctx.NewString(string(respBody)))
			}
			return obj

		case "load_setting", "load_data":
			return ctx.Null()
		case "isLogged":
			return ctx.Bool(false)
		case "log":
			return ctx.Undefined()
		case "delay":
			return ctx.Undefined()
		default:
			return ctx.ThrowError(fmt.Errorf("sendMessage: unsupported method %q", method))
		}
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

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c == 0 || (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
			return false
		}
	}
	return true
}
