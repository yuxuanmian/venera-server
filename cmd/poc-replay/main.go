package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/buke/quickjs-go"

	"venera-server/internal/jsbridge"
)

type recordedRequest struct {
	Method           string            `json:"method"`
	URL              string            `json:"url"`
	RequestBody      string            `json:"request_body,omitempty"`
	ResponseStatus   int               `json:"response_status"`
	ResponseHeaders  map[string]string `json:"response_headers,omitempty"`
	ResponseBodyBase string            `json:"response_body_base64,omitempty"`
	ResponseBodyText string            `json:"response_body_text,omitempty"`
}

func loadRecordings(path string) ([]recordedRequest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []recordedRequest
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 20*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var rec recordedRequest
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("bad jsonl line: %w", err)
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

func findRecording(recs []recordedRequest, method, url, body string) *recordedRequest {
	for i := range recs {
		r := &recs[i]
		if r.Method != method || r.URL != url {
			continue
		}
		if body != "" && r.RequestBody != body {
			continue
		}
		return r
	}
	return nil
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

func headersToJS(ctx *quickjs.Context, h map[string]string) *quickjs.Value {
	obj := ctx.NewObject()
	for k, v := range h {
		obj.Set(k, ctx.NewString(v))
	}
	return obj
}

func main() {
	if len(os.Args) < 6 {
		fmt.Fprintln(os.Stderr, "usage: poc-replay <init.js> <script.js> <className> <comicId> <recordings.jsonl>")
		os.Exit(2)
	}
	initPath := os.Args[1]
	scriptPath := os.Args[2]
	className := os.Args[3]
	comicID := os.Args[4]
	recordingsPath := os.Args[5]

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
	recordings, err := loadRecordings(recordingsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load recordings: %v\n", err)
		os.Exit(1)
	}
	if len(recordings) == 0 {
		fmt.Fprintln(os.Stderr, "no recordings loaded")
		os.Exit(1)
	}

	runtime := quickjs.NewRuntime()
	defer runtime.Close()
	ctx := runtime.NewContext()
	defer ctx.Close()

	bridge := &jsbridge.Bridge{
		HTTP: func(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value {
			httpMethodVal := msg.Get("http_method")
			defer httpMethodVal.Free()
			httpMethod := httpMethodVal.String()
			urlVal := msg.Get("url")
			defer urlVal.Free()
			url := urlVal.String()
			body := ""
			dataVal := msg.Get("data")
			if dataVal != nil {
				defer dataVal.Free()
			}
			if dataVal != nil && !dataVal.IsNull() && !dataVal.IsUndefined() {
				body = dataVal.String()
			}
			rec := findRecording(recordings, httpMethod, url, body)
			if rec == nil {
				return ctx.ThrowError(fmt.Errorf("no recording for %s %s body=%q", httpMethod, url, body))
			}
			bodyBytes := []byte(rec.ResponseBodyText)
			if rec.ResponseBodyBase != "" {
				bodyBytes, err = base64.StdEncoding.DecodeString(rec.ResponseBodyBase)
				if err != nil {
					return ctx.ThrowError(fmt.Errorf("decode recorded body: %w", err))
				}
			}
			obj := ctx.NewObject()
			obj.Set("status", ctx.NewInt32(int32(rec.ResponseStatus)))
			obj.Set("headers", headersToJS(ctx, rec.ResponseHeaders))
			bytesFlag := msg.Get("bytes")
			if bytesFlag != nil {
				defer bytesFlag.Free()
			}
			if bytesFlag != nil && bytesFlag.Bool() {
				obj.Set("body", ctx.NewArrayBuffer(bodyBytes))
			} else {
				obj.Set("body", ctx.NewString(string(bodyBytes)))
			}
			return obj
		},
		GetData:     func(key, dataKey string) any { return nil },
		GetSetting:  func(key, settingKey string) any { return nil },
		IsLogged:    func(key string) bool { return false },
		GetLocale:   func() string { return "zh_CN" },
		GetPlatform: func() string { return "windows" },
		Log:         func(level, title, content string) {},
	}

	sendMessage := ctx.NewFunction(func(ctx *quickjs.Context, this *quickjs.Value, args []*quickjs.Value) *quickjs.Value {
		if len(args) == 0 || args[0] == nil {
			return ctx.ThrowError(fmt.Errorf("sendMessage: missing args"))
		}
		return bridge.Handle(ctx, args[0])
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
		const replacer = (k, v) => {
			if (v instanceof Map) return Object.fromEntries(v);
			return v === undefined ? null : v;
		};
		return JSON.stringify(res, replacer);
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
