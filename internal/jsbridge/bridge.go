package jsbridge

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/andybalholm/cascadia"
	"github.com/buke/quickjs-go"
	"github.com/google/uuid"
	"golang.org/x/net/html"
)

// Bridge implements the host side of Venera's sendMessage protocol for Go.
// HTTP and source-data methods are pluggable so replay tools and the real
// server engine can share one implementation.
type Bridge struct {
	HTTP        func(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value
	GetData     func(key, dataKey string) any
	SetData     func(key, dataKey string, data any) error
	DeleteData  func(key, dataKey string) error
	GetSetting  func(key, settingKey string) any
	IsLogged    func(key string) bool
	GetLocale   func() string
	GetPlatform func() string
	Cookie      func(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value
	Log         func(level, title, content string)

	docs      map[int]*htmlDoc
	nextDocID int
}

type htmlDoc struct {
	root     *html.Node
	elements []*html.Node
}

func (b *Bridge) doc(id int) *htmlDoc {
	if b.docs == nil {
		return nil
	}
	return b.docs[id]
}

func (b *Bridge) allocDoc() int {
	if b.docs == nil {
		b.docs = map[int]*htmlDoc{}
	}
	id := b.nextDocID
	b.nextDocID++
	b.docs[id] = &htmlDoc{}
	return id
}

func (b *Bridge) allocElement(doc *htmlDoc, n *html.Node) int {
	doc.elements = append(doc.elements, n)
	return len(doc.elements) - 1
}

func (b *Bridge) element(doc *htmlDoc, key int) *html.Node {
	if doc == nil || key < 0 || key >= len(doc.elements) {
		return nil
	}
	return doc.elements[key]
}

// Handle dispatches one sendMessage call.
func (b *Bridge) Handle(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value {
	if msg == nil {
		return ctx.ThrowError(fmt.Errorf("sendMessage: missing args"))
	}
	methodVal := msg.Get("method")
	if methodVal == nil {
		return ctx.ThrowError(fmt.Errorf("sendMessage: missing method"))
	}
	defer methodVal.Free()
	method := methodVal.String()

	switch method {
	case "http":
		if b.HTTP != nil {
			return b.HTTP(ctx, msg)
		}
		return ctx.ThrowError(fmt.Errorf("sendMessage: http handler not configured"))
	case "convert":
		return handleConvert(ctx, msg)
	case "html":
		return b.handleHTML(ctx, msg)
	case "random":
		return handleRandom(ctx, msg)
	case "uuid":
		return ctx.NewString(uuid.NewString())
	case "getLocale":
		if b.GetLocale != nil {
			return ctx.NewString(b.GetLocale())
		}
		return ctx.NewString("en_US")
	case "getPlatform":
		if b.GetPlatform != nil {
			return ctx.NewString(b.GetPlatform())
		}
		return ctx.NewString("windows")
	case "log":
		if b.Log != nil {
			level := msg.Get("level").String()
			title := msg.Get("title").String()
			content := msg.Get("content").String()
			b.Log(level, title, content)
		}
		return ctx.Undefined()
	case "delay":
		timeVal := msg.Get("time")
		ms := int64(0)
		if timeVal != nil {
			ms = timeVal.ToInt64()
			timeVal.Free()
		}
		return ctx.NewPromise(func(resolve, reject func(*quickjs.Value)) {
			go func() {
				time.Sleep(time.Duration(ms) * time.Millisecond)
				ctx.Schedule(func(inner *quickjs.Context) {
					resolve(inner.Undefined())
				})
			}()
		})
	case "load_data":
		key := msg.Get("key").String()
		dataKey := msg.Get("data_key").String()
		if b.GetData != nil {
			return toJS(ctx, b.GetData(key, dataKey))
		}
		return ctx.Null()
	case "save_data":
		key := msg.Get("key").String()
		dataKey := msg.Get("data_key").String()
		data := msg.Get("data")
		if b.SetData != nil {
			if err := b.SetData(key, dataKey, jsValueToAny(data)); err != nil {
				return ctx.ThrowError(err)
			}
		}
		return ctx.Undefined()
	case "delete_data":
		key := msg.Get("key").String()
		dataKey := msg.Get("data_key").String()
		if b.DeleteData != nil {
			if err := b.DeleteData(key, dataKey); err != nil {
				return ctx.ThrowError(err)
			}
		}
		return ctx.Undefined()
	case "load_setting":
		key := msg.Get("key").String()
		settingKey := msg.Get("setting_key").String()
		if b.GetSetting != nil {
			return toJS(ctx, b.GetSetting(key, settingKey))
		}
		return ctx.Null()
	case "isLogged":
		key := msg.Get("key").String()
		logged := false
		if b.IsLogged != nil {
			logged = b.IsLogged(key)
		}
		return ctx.Bool(logged)
	case "cookie":
		if b.Cookie != nil {
			return b.Cookie(ctx, msg)
		}
		// Default: no cookies.
		functionVal := msg.Get("function")
		if functionVal != nil {
			defer functionVal.Free()
			if functionVal.String() == "get" {
				return toJS(ctx, []any{})
			}
		}
		return ctx.Null()
	default:
		return ctx.ThrowError(fmt.Errorf("sendMessage: unsupported method %q", method))
	}
}

func ValueToBytes(v *quickjs.Value) ([]byte, error) {
	if v == nil || v.IsNull() || v.IsUndefined() {
		return []byte{}, nil
	}
	if v.IsByteArray() {
		n := v.ByteLen()
		return v.ToByteArray(uint(n))
	}
	return []byte(v.String()), nil
}

func handleConvert(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value {
	typeVal := msg.Get("type")
	if typeVal == nil {
		return ctx.ThrowError(fmt.Errorf("convert: missing type"))
	}
	defer typeVal.Free()
	typ := typeVal.String()
	isEncodeVal := msg.Get("isEncode")
	if isEncodeVal != nil {
		defer isEncodeVal.Free()
	}
	isEncode := isEncodeVal != nil && isEncodeVal.Bool()
	val := msg.Get("value")
	if val != nil {
		defer val.Free()
	}

	switch typ {
	case "utf8":
		b, err := ValueToBytes(val)
		if err != nil {
			return ctx.ThrowError(err)
		}
		if isEncode {
			return ctx.NewArrayBuffer(b)
		}
		return ctx.NewString(string(b))
	case "base64":
		if isEncode {
			b, err := ValueToBytes(val)
			if err != nil {
				return ctx.ThrowError(err)
			}
			return ctx.NewString(base64.StdEncoding.EncodeToString(b))
		}
		if val == nil {
			return ctx.ThrowError(fmt.Errorf("convert: base64 decode missing value"))
		}
		b, err := base64.StdEncoding.DecodeString(val.String())
		if err != nil {
			return ctx.ThrowError(fmt.Errorf("convert: base64 decode: %w", err))
		}
		return ctx.NewArrayBuffer(b)
	case "md5", "sha1", "sha256", "sha512":
		b, err := ValueToBytes(val)
		if err != nil {
			return ctx.ThrowError(err)
		}
		var h hash.Hash
		switch typ {
		case "md5":
			h = md5.New()
		case "sha1":
			h = sha1.New()
		case "sha256":
			h = sha256.New()
		case "sha512":
			h = sha512.New()
		}
		_, _ = h.Write(b)
		return ctx.NewArrayBuffer(h.Sum(nil))
	case "hmac":
		keyVal := msg.Get("key")
		if keyVal != nil {
			defer keyVal.Free()
		}
		key, err := ValueToBytes(keyVal)
		if err != nil {
			return ctx.ThrowError(err)
		}
		b, err := ValueToBytes(val)
		if err != nil {
			return ctx.ThrowError(err)
		}
		hashName := msg.Get("hash").String()
		var h func() hash.Hash
		switch hashName {
		case "md5":
			h = md5.New
		case "sha1":
			h = sha1.New
		case "sha256":
			h = sha256.New
		case "sha512":
			h = sha512.New
		default:
			return ctx.ThrowError(fmt.Errorf("convert: unsupported hmac hash %q", hashName))
		}
		mac := hmac.New(h, key)
		_, _ = mac.Write(b)
		sum := mac.Sum(nil)
		isStringVal := msg.Get("isString")
		isString := isStringVal != nil && isStringVal.Bool()
		if isStringVal != nil {
			defer isStringVal.Free()
		}
		if isString {
			return ctx.NewString(hex.EncodeToString(sum))
		}
		return ctx.NewArrayBuffer(sum)
	default:
		return ctx.ThrowError(fmt.Errorf("convert: unsupported type %q", typ))
	}
}

func handleRandom(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value {
	minVal := msg.Get("min")
	maxVal := msg.Get("max")
	typeVal := msg.Get("type")
	if minVal != nil {
		defer minVal.Free()
	}
	if maxVal != nil {
		defer maxVal.Free()
	}
	if typeVal != nil {
		defer typeVal.Free()
	}
	min := float64(0)
	max := float64(1)
	if minVal != nil && !minVal.IsUndefined() && !minVal.IsNull() {
		min = minVal.ToFloat64()
	}
	if maxVal != nil && !maxVal.IsUndefined() && !maxVal.IsNull() {
		max = maxVal.ToFloat64()
	}
	typ := ""
	if typeVal != nil && !typeVal.IsUndefined() && !typeVal.IsNull() {
		typ = typeVal.String()
	}
	if typ == "double" {
		return ctx.NewFloat64(min + rand.Float64()*(max-min))
	}
	return ctx.NewInt32(int32(min + rand.Float64()*(max-min)))
}

func toJS(ctx *quickjs.Context, v any) *quickjs.Value {
	switch x := v.(type) {
	case nil:
		return ctx.Null()
	case string:
		return ctx.NewString(x)
	case bool:
		return ctx.Bool(x)
	case int:
		return ctx.NewInt32(int32(x))
	case int32:
		return ctx.NewInt32(x)
	case int64:
		return ctx.NewInt64(x)
	case float64:
		return ctx.NewFloat64(x)
	case []any:
		arr := ctx.Eval("[]")
		for i, item := range x {
			arr.Set(strconv.Itoa(i), toJS(ctx, item))
		}
		return arr
	case []string:
		arr := ctx.Eval("[]")
		for i, item := range x {
			arr.Set(strconv.Itoa(i), ctx.NewString(item))
		}
		return arr
	case map[string]any:
		obj := ctx.NewObject()
		for k, item := range x {
			obj.Set(k, toJS(ctx, item))
		}
		return obj
	default:
		return ctx.Null()
	}
}

func jsValueToAny(v *quickjs.Value) any {
	if v == nil || v.IsNull() || v.IsUndefined() {
		return nil
	}
	if v.IsString() {
		return v.String()
	}
	if v.IsBool() {
		return v.Bool()
	}
	if v.IsNumber() {
		return v.ToFloat64()
	}
	if v.IsByteArray() {
		b, _ := ValueToBytes(v)
		return b
	}
	if v.IsArray() {
		n := v.Len()
		out := make([]any, 0, n)
		for i := int64(0); i < n; i++ {
			item := v.Get(strconv.FormatInt(i, 10))
			if item != nil {
				out = append(out, jsValueToAny(item))
				item.Free()
			}
		}
		return out
	}
	if v.IsObject() {
		names, err := v.PropertyNames()
		if err != nil {
			return nil
		}
		out := map[string]any{}
		for _, name := range names {
			item := v.Get(name)
			if item != nil {
				out[name] = jsValueToAny(item)
				item.Free()
			}
		}
		return out
	}
	return v.String()
}

func (b *Bridge) handleHTML(ctx *quickjs.Context, msg *quickjs.Value) *quickjs.Value {
	fnVal := msg.Get("function")
	if fnVal == nil {
		return ctx.ThrowError(fmt.Errorf("html: missing function"))
	}
	defer fnVal.Free()
	fn := fnVal.String()

	keyVal := msg.Get("key")
	var key int
	if keyVal != nil {
		key = int(keyVal.ToInt64())
		keyVal.Free()
	}

	switch fn {
	case "parse":
		dataVal := msg.Get("data")
		if dataVal == nil {
			return ctx.ThrowError(fmt.Errorf("html: parse missing data"))
		}
		doc, err := html.Parse(strings.NewReader(dataVal.String()))
		dataVal.Free()
		if err != nil {
			return ctx.ThrowError(fmt.Errorf("html: parse: %w", err))
		}
		id := b.allocDoc()
		b.docs[id].root = doc
		return ctx.NewInt32(int32(id))
	case "dispose":
		if b.docs != nil {
			delete(b.docs, key)
		}
		return ctx.Undefined()
	}

	docID := key
	docVal := msg.Get("doc")
	if docVal != nil {
		docID = int(docVal.ToInt64())
		docVal.Free()
	}
	doc := b.doc(docID)
	if doc == nil {
		return ctx.ThrowError(fmt.Errorf("html: unknown document key %d", docID))
	}

	switch fn {
	case "querySelector":
		queryVal := msg.Get("query")
		query := queryVal.String()
		queryVal.Free()
		n := cascadia.Query(doc.root, mustCompile(query))
		if n == nil {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(b.allocElement(doc, n)))
	case "querySelectorAll":
		queryVal := msg.Get("query")
		query := queryVal.String()
		queryVal.Free()
		nodes := cascadia.QueryAll(doc.root, mustCompile(query))
		return b.nodesToJSArray(ctx, doc, nodes)
	case "getElementById":
		idVal := msg.Get("id")
		if idVal == nil {
			return ctx.ThrowError(fmt.Errorf("html: getElementById missing id"))
		}
		id := idVal.String()
		idVal.Free()
		n := findElementByID(doc.root, id)
		if n == nil {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(b.allocElement(doc, n)))
	case "getText":
		n := b.element(doc, key)
		if n == nil {
			return ctx.Null()
		}
		return ctx.NewString(nodeText(n))
	case "getAttributes":
		n := b.element(doc, key)
		if n == nil {
			return ctx.NewObject()
		}
		obj := ctx.NewObject()
		for _, attr := range n.Attr {
			obj.Set(attr.Key, ctx.NewString(attr.Val))
		}
		return obj
	case "getInnerHTML":
		n := b.element(doc, key)
		if n == nil {
			return ctx.NewString("")
		}
		var sb strings.Builder
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			_ = html.Render(&sb, c)
		}
		// x/net/html renders void elements as <br/>; Dart's html package keeps <br>.
		return ctx.NewString(strings.ReplaceAll(sb.String(), "<br/>", "<br>"))
	case "getParent":
		n := b.element(doc, key)
		if n == nil || n.Parent == nil {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(b.allocElement(doc, n.Parent)))
	case "getChildren":
		n := b.element(doc, key)
		if n == nil {
			return toJS(ctx, []any{})
		}
		var nodes []*html.Node
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode {
				nodes = append(nodes, c)
			}
		}
		return b.nodesToJSArray(ctx, doc, nodes)
	case "getNodes":
		n := b.element(doc, key)
		if n == nil {
			return toJS(ctx, []any{})
		}
		var nodes []*html.Node
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			nodes = append(nodes, c)
		}
		return b.nodesToJSArray(ctx, doc, nodes)
	case "dom_querySelector":
		n := b.element(doc, key)
		if n == nil {
			return ctx.Null()
		}
		queryVal := msg.Get("query")
		query := queryVal.String()
		queryVal.Free()
		child := cascadia.Query(n, mustCompile(query))
		if child == nil {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(b.allocElement(doc, child)))
	case "dom_querySelectorAll":
		n := b.element(doc, key)
		if n == nil {
			return toJS(ctx, []any{})
		}
		queryVal := msg.Get("query")
		query := queryVal.String()
		queryVal.Free()
		nodes := cascadia.QueryAll(n, mustCompile(query))
		return b.nodesToJSArray(ctx, doc, nodes)
	case "getClassNames":
		n := b.element(doc, key)
		if n == nil {
			return toJS(ctx, []any{})
		}
		var classes []string
		for _, attr := range n.Attr {
			if attr.Key == "class" {
				classes = strings.Fields(attr.Val)
				break
			}
		}
		return toJS(ctx, classes)
	case "getId":
		n := b.element(doc, key)
		if n == nil {
			return ctx.Null()
		}
		for _, attr := range n.Attr {
			if attr.Key == "id" {
				return ctx.NewString(attr.Val)
			}
		}
		return ctx.Null()
	case "getLocalName":
		n := b.element(doc, key)
		if n == nil {
			return ctx.Null()
		}
		return ctx.NewString(n.Data)
	case "getPreviousSibling":
		n := b.element(doc, key)
		if n == nil || n.PrevSibling == nil || n.PrevSibling.Type != html.ElementNode {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(b.allocElement(doc, n.PrevSibling)))
	case "getNextSibling":
		n := b.element(doc, key)
		if n == nil || n.NextSibling == nil || n.NextSibling.Type != html.ElementNode {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(b.allocElement(doc, n.NextSibling)))
	default:
		return ctx.ThrowError(fmt.Errorf("html: unsupported function %q", fn))
	}
}

func (b *Bridge) nodesToJSArray(ctx *quickjs.Context, doc *htmlDoc, nodes []*html.Node) *quickjs.Value {
	arr := ctx.Eval("[]")
	for i, n := range nodes {
		arr.Set(strconv.Itoa(i), ctx.NewInt32(int32(b.allocElement(doc, n))))
	}
	return arr
}

type emptySelector struct{}

func (emptySelector) Match(*html.Node) bool             { return false }
func (emptySelector) Specificity() cascadia.Specificity { return cascadia.Specificity{} }
func (emptySelector) String() string                    { return "" }
func (emptySelector) PseudoElement() string             { return "" }

func mustCompile(sel string) cascadia.Sel {
	s, err := cascadia.Parse(sel)
	if err != nil {
		// Fallback: match nothing instead of crashing the whole runtime.
		return emptySelector{}
	}
	return s
}

func findElementByID(n *html.Node, id string) *html.Node {
	if n.Type == html.ElementNode {
		for _, attr := range n.Attr {
			if attr.Key == "id" && attr.Val == id {
				return n
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if r := findElementByID(c, id); r != nil {
			return r
		}
	}
	return nil
}

func nodeText(n *html.Node) string {
	var sb strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			sb.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return sb.String()
}
