package worker

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/andybalholm/cascadia"
	"github.com/buke/quickjs-go"
	"golang.org/x/net/html"
)

type workerHTMLBridge struct {
	docs   map[int]*workerHTMLDocument
	nextID int
}

type workerHTMLDocument struct {
	root     *html.Node
	elements []*html.Node
}

func newWorkerHTMLBridge() *workerHTMLBridge {
	return &workerHTMLBridge{docs: make(map[int]*workerHTMLDocument)}
}

func (bridge *workerHTMLBridge) Handle(ctx *quickjs.Context, message *quickjs.Value) *quickjs.Value {
	if message == nil {
		return ctx.ThrowError(fmt.Errorf("html: missing message"))
	}
	functionValue := message.Get("function")
	if functionValue == nil {
		return ctx.ThrowError(fmt.Errorf("html: missing function"))
	}
	function := functionValue.String()
	functionValue.Free()

	keyValue := message.Get("key")
	hasKey := keyValue != nil && !keyValue.IsUndefined() && !keyValue.IsNull()
	key := valueInt(keyValue)
	if function == "parse" {
		dataValue := message.Get("data")
		if dataValue == nil {
			return ctx.ThrowError(fmt.Errorf("html: parse missing data"))
		}
		data := dataValue.String()
		dataValue.Free()
		root, err := html.Parse(strings.NewReader(data))
		if err != nil {
			return ctx.ThrowError(fmt.Errorf("html: parse: %w", err))
		}
		if !hasKey {
			key = bridge.nextID
			bridge.nextID++
		}
		bridge.docs[key] = &workerHTMLDocument{root: root}
		return ctx.NewInt32(int32(key))
	}
	if function == "dispose" {
		delete(bridge.docs, key)
		return ctx.Undefined()
	}

	documentID := key
	documentValue := message.Get("doc")
	if documentValue != nil && !documentValue.IsUndefined() && !documentValue.IsNull() {
		documentID = int(documentValue.ToInt64())
		documentValue.Free()
	}
	document := bridge.docs[documentID]
	if document == nil {
		return ctx.ThrowError(fmt.Errorf("html: unknown document key %d", documentID))
	}

	switch function {
	case "querySelector", "querySelectorAll", "getElementById":
		return bridge.documentQuery(ctx, message, documentID, document, function)
	case "getText", "getAttributes", "getInnerHTML", "getParent", "getChildren", "getNodes",
		"getClassNames", "getId", "getLocalName", "getPreviousSibling", "getNextSibling":
		return bridge.elementOperation(ctx, message, documentID, document, key, function)
	case "dom_querySelector", "dom_querySelectorAll":
		element := bridge.element(document, key)
		if element == nil {
			return ctx.Null()
		}
		queryValue := message.Get("query")
		if queryValue == nil {
			return ctx.ThrowError(fmt.Errorf("html: missing query"))
		}
		query := queryValue.String()
		queryValue.Free()
		if function == "dom_querySelector" {
			node := cascadia.Query(element, compileWorkerSelector(query))
			if node == nil {
				return ctx.Null()
			}
			return ctx.NewInt32(int32(bridge.allocElement(document, node)))
		}
		return bridge.nodesToJSArray(ctx, document, cascadia.QueryAll(element, compileWorkerSelector(query)))
	default:
		return ctx.ThrowError(fmt.Errorf("html: unsupported function %q", function))
	}
}

func (bridge *workerHTMLBridge) documentQuery(ctx *quickjs.Context, message *quickjs.Value, documentID int, document *workerHTMLDocument, function string) *quickjs.Value {
	if function == "getElementById" {
		idValue := message.Get("id")
		if idValue == nil {
			return ctx.ThrowError(fmt.Errorf("html: getElementById missing id"))
		}
		id := idValue.String()
		idValue.Free()
		node := findWorkerElementByID(document.root, id)
		if node == nil {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(bridge.allocElement(document, node)))
	}
	queryValue := message.Get("query")
	if queryValue == nil {
		return ctx.ThrowError(fmt.Errorf("html: missing query"))
	}
	query := queryValue.String()
	queryValue.Free()
	selector := compileWorkerSelector(query)
	if function == "querySelector" {
		node := cascadia.Query(document.root, selector)
		if node == nil {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(bridge.allocElement(document, node)))
	}
	return bridge.nodesToJSArray(ctx, document, cascadia.QueryAll(document.root, selector))
}

func (bridge *workerHTMLBridge) elementOperation(ctx *quickjs.Context, message *quickjs.Value, documentID int, document *workerHTMLDocument, key int, function string) *quickjs.Value {
	node := bridge.element(document, key)
	switch function {
	case "getText":
		if node == nil {
			return ctx.Null()
		}
		return ctx.NewString(workerNodeText(node))
	case "getAttributes":
		result := ctx.NewObject()
		if node != nil {
			for _, attribute := range node.Attr {
				result.Set(attribute.Key, ctx.NewString(attribute.Val))
			}
		}
		return result
	case "getInnerHTML":
		if node == nil {
			return ctx.NewString("")
		}
		var builder strings.Builder
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			_ = html.Render(&builder, child)
		}
		return ctx.NewString(strings.ReplaceAll(builder.String(), "<br/>", "<br>"))
	case "getParent":
		if node == nil || node.Parent == nil {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(bridge.allocElement(document, node.Parent)))
	case "getChildren":
		if node == nil {
			return workerToJS(ctx, []any{})
		}
		children := make([]*html.Node, 0)
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			if child.Type == html.ElementNode {
				children = append(children, child)
			}
		}
		return bridge.nodesToJSArray(ctx, document, children)
	case "getNodes":
		if node == nil {
			return workerToJS(ctx, []any{})
		}
		children := make([]*html.Node, 0)
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			children = append(children, child)
		}
		return bridge.nodesToJSArray(ctx, document, children)
	case "getClassNames":
		if node == nil {
			return workerToJS(ctx, []any{})
		}
		for _, attribute := range node.Attr {
			if attribute.Key == "class" {
				return workerToJS(ctx, strings.Fields(attribute.Val))
			}
		}
		return workerToJS(ctx, []any{})
	case "getId":
		if node == nil {
			return ctx.Null()
		}
		for _, attribute := range node.Attr {
			if attribute.Key == "id" {
				return ctx.NewString(attribute.Val)
			}
		}
		return ctx.Null()
	case "getLocalName":
		if node == nil {
			return ctx.Null()
		}
		return ctx.NewString(node.Data)
	case "getPreviousSibling":
		if node == nil || node.PrevSibling == nil || node.PrevSibling.Type != html.ElementNode {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(bridge.allocElement(document, node.PrevSibling)))
	case "getNextSibling":
		if node == nil || node.NextSibling == nil || node.NextSibling.Type != html.ElementNode {
			return ctx.Null()
		}
		return ctx.NewInt32(int32(bridge.allocElement(document, node.NextSibling)))
	default:
		return ctx.ThrowError(fmt.Errorf("html: unsupported function %q", function))
	}
}

func (bridge *workerHTMLBridge) allocElement(document *workerHTMLDocument, node *html.Node) int {
	document.elements = append(document.elements, node)
	return len(document.elements) - 1
}

func (bridge *workerHTMLBridge) element(document *workerHTMLDocument, key int) *html.Node {
	if document == nil || key < 0 || key >= len(document.elements) {
		return nil
	}
	return document.elements[key]
}

func (bridge *workerHTMLBridge) nodesToJSArray(ctx *quickjs.Context, document *workerHTMLDocument, nodes []*html.Node) *quickjs.Value {
	array := ctx.Eval("[]")
	for index, node := range nodes {
		array.Set(strconv.Itoa(index), ctx.NewInt32(int32(bridge.allocElement(document, node))))
	}
	return array
}

func compileWorkerSelector(selector string) cascadia.Sel {
	compiled, err := cascadia.Parse(selector)
	if err != nil {
		return workerEmptySelector{}
	}
	return compiled
}

type workerEmptySelector struct{}

func (workerEmptySelector) Match(*html.Node) bool             { return false }
func (workerEmptySelector) Specificity() cascadia.Specificity { return cascadia.Specificity{} }
func (workerEmptySelector) String() string                    { return "" }
func (workerEmptySelector) PseudoElement() string             { return "" }

func findWorkerElementByID(node *html.Node, id string) *html.Node {
	if node == nil {
		return nil
	}
	if node.Type == html.ElementNode {
		for _, attribute := range node.Attr {
			if attribute.Key == "id" && attribute.Val == id {
				return node
			}
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if result := findWorkerElementByID(child, id); result != nil {
			return result
		}
	}
	return nil
}

func workerNodeText(node *html.Node) string {
	var builder strings.Builder
	var visit func(*html.Node)
	visit = func(current *html.Node) {
		if current.Type == html.TextNode {
			builder.WriteString(current.Data)
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	if node != nil {
		visit(node)
	}
	return builder.String()
}

func valueInt(value *quickjs.Value) int {
	if value == nil {
		return 0
	}
	result := int(value.ToInt64())
	value.Free()
	return result
}

func workerToJS(ctx *quickjs.Context, value any) *quickjs.Value {
	switch typed := value.(type) {
	case []any:
		array := ctx.Eval("[]")
		for index, item := range typed {
			array.Set(strconv.Itoa(index), workerToJS(ctx, item))
		}
		return array
	default:
		return ctx.Null()
	}
}
