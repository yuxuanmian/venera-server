package engine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buke/quickjs-go"

	"venera-server/internal/config"
	"venera-server/internal/cryptoutil"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/interval"
	"venera-server/internal/jsbridge"
	"venera-server/internal/store"
)

type Engine struct {
	store     *store.Store
	cfg       *config.Config
	rec       *debugrecorder.Recorder
	client    *http.Client
	initJS    []byte
	cookieKey []byte
}

func New(cfg *config.Config, st *store.Store, rec *debugrecorder.Recorder) (*Engine, error) {
	initJS, err := os.ReadFile(cfg.InitJSPath)
	if err != nil {
		return nil, fmt.Errorf("read init.js: %w", err)
	}
	key, err := cryptoutil.LoadOrCreateKeyWithOverride(cfg.DataDir, cfg.CookieKey)
	if err != nil {
		return nil, fmt.Errorf("load cookie key: %w", err)
	}
	return &Engine{
		store:     st,
		cfg:       cfg,
		rec:       rec,
		client:    &http.Client{Timeout: 30 * time.Second},
		initJS:    initJS,
		cookieKey: key,
	}, nil
}

// RunJob executes a single pending job.
func (e *Engine) RunJob(ctx context.Context, job store.Job) error {
	claimed, err := e.store.ClaimJob(job.JobID)
	if err != nil {
		return err
	}
	if !claimed {
		return nil // another worker took it
	}

	scriptCode, scriptPath, err := e.loadScript(job)
	if err != nil {
		_ = e.store.FailJob(job.JobID, fmt.Sprintf("read script: %v", err), false)
		return fmt.Errorf("read script %s: %w", scriptPath, err)
	}

	resultJSON, outcome, err := e.executeLoadInfo(job, string(scriptCode))
	scanTime := time.Now().UTC().Format(time.RFC3339)

	if err != nil {
		_ = e.insertErrorResult(job, scanTime, outcome, err.Error())
		switch outcome {
		case "needs_relogin":
			_ = e.store.MarkNeedsResubmit(job.JobID, err.Error())
		case "not_found":
			_ = e.store.FailJob(job.JobID, err.Error(), true)
		default:
			_ = e.store.FailJob(job.JobID, err.Error(), true)
		}
		return err
	}

	if err := e.insertSuccessResult(job, scanTime, resultJSON); err != nil {
		_ = e.store.FailJob(job.JobID, err.Error(), true)
		return err
	}
	_ = e.store.CompleteJob(job.JobID)
	return nil
}

func (e *Engine) loadScript(job store.Job) ([]byte, string, error) {
	// Prefer the hash-keyed script uploaded through /api/scripts.
	if sess, err := e.store.GetSourceSession(job.UserID, job.Source); err == nil && sess.ScriptHash != "" {
		if p, err := e.store.GetScriptPath(sess.ScriptHash); err == nil {
			if data, err := os.ReadFile(p); err == nil {
				return data, p, nil
			}
		}
	}
	// Local development fallback.
	scriptPath := filepath.Join(e.cfg.ComicSourceDir, job.Source+".js")
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		return nil, scriptPath, err
	}
	return data, scriptPath, nil
}

func (e *Engine) executeLoadInfo(job store.Job, scriptCode string) (string, string, error) {
	runtime := quickjs.NewRuntime()
	defer runtime.Close()
	ctx := runtime.NewContext()
	defer ctx.Close()

	// Optional cookie, UA, proxy and init_js from source session.
	var cookieHeader string
	ua := ""
	initJS := e.initJS
	if sess, err := e.store.GetSourceSession(job.UserID, job.Source); err == nil {
		if sess.CookieEncrypted != "" {
			if plain, err := cryptoutil.Decrypt(sess.CookieEncrypted, e.cookieKey); err == nil && plain != "" {
				cookieHeader = plain
			}
		}
		ua = sess.UA
		if sess.InitJSHash != "" {
			if p, err := e.store.GetScriptPath(sess.InitJSHash); err == nil {
				if data, err := os.ReadFile(p); err == nil {
					initJS = data
				}
			}
		}
	}

	bridge := &jsbridge.Bridge{
		HTTP: func(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value {
			return e.handleHTTP(ctx, msg, cookieHeader, ua)
		},
		GetData:     func(key, dataKey string) any { return nil },
		GetSetting:  func(key, settingKey string) any { return nil },
		IsLogged:    func(key string) bool { return cookieHeader != "" },
		GetLocale:   func() string { return "zh_CN" },
		GetPlatform: func() string { return "windows" },
		Log:         func(level, title, content string) {},
		Cookie: func(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value {
			return cookieBridge(ctx, msg, cookieHeader)
		},
	}

	sendMessage := ctx.NewFunction(func(ctx *quickjs.Context, this *quickjs.Value, args []*quickjs.Value) *quickjs.Value {
		if len(args) == 0 || args[0] == nil {
			return ctx.ThrowError(fmt.Errorf("sendMessage: missing args"))
		}
		return bridge.Handle(ctx, args[0])
	})
	ctx.Globals().Set("sendMessage", sendMessage)

	if v := ctx.Eval(string(initJS)); v == nil || v.IsException() {
		if v != nil {
			defer v.Free()
		}
		return "", "error", ctx.Exception()
	} else {
		v.Free()
	}

	wrapper := fmt.Sprintf("(() => { %s\n this['temp'] = new %s()\n })()", scriptCode, classNameForScript(scriptCode))
	if v := ctx.Eval(wrapper); v == nil || v.IsException() {
		if v != nil {
			defer v.Free()
		}
		return "", "error", ctx.Exception()
	} else {
		v.Free()
	}

	expr := fmt.Sprintf(`(async () => {
		const s = this['temp'];
		let res;
		try {
			res = await s.comic.loadInfo(%q);
		} catch (e) {
			return JSON.stringify({ __error: String(e && e.message ? e.message : e) });
		}
		const replacer = (k, v) => {
			if (v instanceof Map) return Object.fromEntries(v);
			return v === undefined ? null : v;
		};
		return JSON.stringify(res, replacer);
	})()`, job.ComicID)
	v := ctx.Eval(expr, quickjs.EvalAwait(true))
	if v == nil {
		return "", "error", errors.New("eval returned nil")
	}
	defer v.Free()
	if v.IsException() {
		errMsg := ctx.Exception().Error()
		return "", classifyError(errMsg), ctx.Exception()
	}
	result := v.String()
	if isErrorEnvelope(result) {
		var env struct {
			Error string `json:"__error"`
		}
		if err := json.Unmarshal([]byte(result), &env); err == nil && env.Error != "" {
			return "", classifyError(env.Error), fmt.Errorf("%s", env.Error)
		}
		return "", "error", errors.New("unknown js error")
	}
	return result, "success", nil
}

func isErrorEnvelope(s string) bool {
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return false
	}
	_, ok := m["__error"]
	return ok
}

func (e *Engine) handleHTTP(ctx *quickjs.Context, msg *quickjs.Value, cookieHeader, ua string) *quickjs.Value {
	httpMethodVal := msg.Get("http_method")
	defer httpMethodVal.Free()
	httpMethod := httpMethodVal.String()
	urlVal := msg.Get("url")
	defer urlVal.Free()
	url := urlVal.String()
	headersVal := msg.Get("headers")
	headers := headersFromJS(ctx, headersVal)
	if headersVal != nil {
		defer headersVal.Free()
	}
	if cookieHeader != "" {
		if _, ok := headers["Cookie"]; !ok {
			headers["Cookie"] = cookieHeader
		}
	}
	if ua != "" {
		if _, ok := headers["User-Agent"]; !ok {
			if _, ok2 := headers["user-agent"]; !ok2 {
				headers["User-Agent"] = ua
			}
		}
	}
	dataVal := msg.Get("data")
	if dataVal != nil {
		defer dataVal.Free()
	}
	var body io.Reader
	bodyStr := ""
	if dataVal != nil && !dataVal.IsNull() && !dataVal.IsUndefined() {
		if dataVal.IsByteArray() {
			b, err := jsbridge.ValueToBytes(dataVal)
			if err != nil {
				return ctx.ThrowError(fmt.Errorf("read body bytes: %w", err))
			}
			body = bytes.NewReader(b)
		} else {
			bodyStr = dataVal.String()
			body = strings.NewReader(bodyStr)
		}
	}

	req, err := http.NewRequest(httpMethod, url, body)
	if err != nil {
		return ctx.ThrowError(fmt.Errorf("new request: %w", err))
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return ctx.ThrowError(fmt.Errorf("http do: %w", err))
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 20<<20))
	if err != nil {
		return ctx.ThrowError(fmt.Errorf("read body: %w", err))
	}

	e.recordHTTP(httpMethod, url, headers, bodyStr, resp, respBody)

	obj := ctx.NewObject()
	obj.Set("status", ctx.NewInt32(int32(resp.StatusCode)))
	obj.Set("headers", headersToJS(ctx, resp.Header))
	bytesFlag := msg.Get("bytes")
	if bytesFlag != nil {
		defer bytesFlag.Free()
	}
	if bytesFlag != nil && bytesFlag.Bool() {
		obj.Set("body", ctx.NewArrayBuffer(respBody))
	} else {
		obj.Set("body", ctx.NewString(string(respBody)))
	}
	return obj
}

func (e *Engine) recordHTTP(method, url string, reqHeaders map[string]string, reqBody string, resp *http.Response, body []byte) {
	if e.rec == nil {
		return
	}
	safeHeaders := make(map[string]string, len(reqHeaders))
	for k, v := range reqHeaders {
		lk := strings.ToLower(k)
		if lk == "cookie" || lk == "authorization" || lk == "set-cookie" || lk == "proxy-authorization" {
			safeHeaders[k] = "[REDACTED]"
			continue
		}
		safeHeaders[k] = v
	}
	entry := map[string]any{
		"ts":               time.Now().UTC().Format(time.RFC3339),
		"method":           method,
		"url":              url,
		"request_headers":  safeHeaders,
		"request_body":     "[REDACTED]",
		"response_status":  resp.StatusCode,
		"response_headers": flattenHeader(resp.Header),
	}
	if isPrintable(body) {
		entry["response_body_text"] = string(body)
	} else {
		entry["response_body_base64"] = base64.StdEncoding.EncodeToString(body)
	}
	_ = e.rec.Record(entry)
}

func (e *Engine) insertSuccessResult(job store.Job, scanTime, resultJSON string) error {
	var payloadMap map[string]any
	_ = json.Unmarshal([]byte(resultJSON), &payloadMap)
	lastUpdate := ""
	if payloadMap != nil {
		if s, ok := payloadMap["updateTime"].(string); ok {
			lastUpdate = s
		}
	}
	inserted, err := e.store.InsertResult(store.Result{
		UserID:   job.UserID,
		Source:   job.Source,
		ComicID:  job.ComicID,
		ScanTime: scanTime,
		Outcome:  "success",
		Payload:  sql.NullString{String: resultJSON, Valid: true},
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	_ = e.store.PrunePayloads(job.UserID, job.Source, job.ComicID)
	nextDue, err := interval.NextDue(time.Now().UTC(), lastUpdate)
	if err != nil {
		nextDue = time.Now().UTC().Add(24 * time.Hour)
	}
	if m, err := e.store.GetMirror(job.UserID, job.Source, job.ComicID); err == nil {
		_ = e.store.MergeMirror(store.Mirror{
			UserID:         job.UserID,
			Source:         job.Source,
			ComicID:        job.ComicID,
			DueAt:          nextDue.UTC().Format(time.RFC3339),
			LastCheckTime:  scanTime,
			LastUpdateTime: lastUpdate,
			Priority:       m.Priority,
			UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
		})
	}
	return nil
}

func (e *Engine) insertErrorResult(job store.Job, scanTime, outcome, detail string) error {
	_, err := e.store.InsertResult(store.Result{
		UserID:   job.UserID,
		Source:   job.Source,
		ComicID:  job.ComicID,
		ScanTime: scanTime,
		Outcome:  outcome,
		Detail:   sql.NullString{String: detail, Valid: detail != ""},
	})
	return err
}

func cookieBridge(ctx *quickjs.Context, msg *quickjs.Value, cookieHeader string) *quickjs.Value {
	fnVal := msg.Get("function")
	if fnVal == nil {
		return ctx.Null()
	}
	defer fnVal.Free()
	if fnVal.String() != "get" {
		return ctx.Null()
	}
	arr := ctx.Eval("[]")
	if cookieHeader == "" {
		return arr
	}
	idx := 0
	for _, part := range strings.Split(cookieHeader, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 0 || strings.TrimSpace(kv[0]) == "" {
			continue
		}
		obj := ctx.NewObject()
		obj.Set("name", ctx.NewString(strings.TrimSpace(kv[0])))
		val := ""
		if len(kv) > 1 {
			val = strings.TrimSpace(kv[1])
		}
		obj.Set("value", ctx.NewString(val))
		arr.Set(strconv.Itoa(idx), obj)
		idx++
	}
	return arr
}

func classNameForScript(script string) string {
	// Very small parser: first line starting with "class X extends ComicSource".
	for _, line := range strings.Split(script, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "class ") && strings.Contains(line, "extends ComicSource") {
			rest := strings.TrimPrefix(line, "class ")
			rest = strings.TrimSpace(rest)
			if idx := strings.Index(rest, "extends"); idx >= 0 {
				return strings.TrimSpace(rest[:idx])
			}
		}
	}
	return ""
}

func classifyError(msg string) string {
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "relogin") || strings.Contains(lower, "login") {
		return "needs_relogin"
	}
	if strings.Contains(lower, "404") || strings.Contains(lower, "not found") {
		return "not_found"
	}
	return "error"
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
			val.Free()
		}
	}
	return out
}

func headersToJS(ctx *quickjs.Context, h http.Header) *quickjs.Value {
	obj := ctx.NewObject()
	for k, vs := range h {
		if len(vs) > 0 {
			obj.Set(k, ctx.NewString(strings.Join(vs, ",")))
		}
	}
	return obj
}

func flattenHeader(h http.Header) map[string]string {
	out := map[string]string{}
	for k, vs := range h {
		if len(vs) > 0 {
			out[k] = strings.Join(vs, ",")
		}
	}
	return out
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c == 0 || (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
			return false
		}
	}
	return true
}
