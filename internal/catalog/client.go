package catalog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	serverCheckTimeout = 20 * time.Second
	resolverTimeout    = 10 * time.Second
	maxResolverBytes   = 128
)

type Client struct {
	HTTPClient *http.Client
}

func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &Client{HTTPClient: httpClient}
}

func (c *Client) ResolveRevision(ctx context.Context, ref CatalogRef) (string, error) {
	if ref.Owner == "" || ref.Repo == "" || ref.Ref == "" {
		return "", errors.New("incomplete catalog ref")
	}
	path := "https://" + GitHubAPIHost + "/repos/" + url.PathEscape(ref.Owner) + "/" + url.PathEscape(ref.Repo) + "/commits/" + url.PathEscape(ref.Ref)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "application/vnd.github.sha")
	request.Header.Set("User-Agent", "Venera-Catalog-Server")
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.Request != nil && response.Request.URL.Host != GitHubAPIHost {
		return "", errors.New("resolver redirected to an untrusted host")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("resolver returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResolverBytes+1))
	if err != nil {
		return "", err
	}
	if len(body) > maxResolverBytes {
		return "", errors.New("resolver response is too large")
	}
	sha := strings.TrimSpace(string(body))
	if !shaPattern.MatchString(sha) {
		return "", errors.New("resolver did not return a full lowercase SHA-1")
	}
	return sha, nil
}

func (c *Client) FetchIndex(ctx context.Context, pointer CatalogPointer) ([]byte, CatalogIndex, error) {
	if err := ValidatePointer(pointer); err != nil {
		return nil, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pointer.IndexURL, nil)
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("User-Agent", "Venera-Catalog-Server")
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	if response.Request != nil && response.Request.URL.Host != RawHost {
		return nil, nil, errors.New("index redirected to an untrusted host")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("index returned HTTP %d", response.StatusCode)
	}
	if response.ContentLength > MaxIndexBytes {
		return nil, nil, errors.New("index exceeds maximum size")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxIndexBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if len(body) > MaxIndexBytes {
		return nil, nil, errors.New("index exceeds maximum size")
	}
	index, err := ParseIndex(body)
	if err != nil {
		return nil, nil, newOperationError("catalog_invalid", "漫画源配置索引无效", err)
	}
	return body, index, nil
}

func (c *Client) FetchCatalog(ctx context.Context, configuredURL string) (CatalogPointer, []byte, CatalogIndex, error) {
	ref, err := ParseConfiguredURL(configuredURL)
	if err != nil {
		return CatalogPointer{}, nil, nil, err
	}
	sha := ref.Ref
	if !shaPattern.MatchString(sha) {
		resolveCtx, cancel := context.WithTimeout(ctx, resolverTimeout)
		defer cancel()
		sha, err = c.ResolveRevision(resolveCtx, ref)
		if err != nil {
			return CatalogPointer{}, nil, nil, newOperationError("catalog_fetch_failed", "无法解析漫画源配置版本", err)
		}
	}
	pointer, err := BuildPinnedPointer(ref, sha)
	if err != nil {
		return CatalogPointer{}, nil, nil, err
	}
	indexCtx, cancel := context.WithTimeout(ctx, serverCheckTimeout)
	defer cancel()
	body, index, err := c.FetchIndex(indexCtx, pointer)
	if err != nil {
		var operationErr *OperationError
		if errors.As(err, &operationErr) {
			return CatalogPointer{}, nil, nil, err
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return CatalogPointer{}, nil, nil, newOperationError("catalog_fetch_timeout", "获取漫画源配置超时", err)
		}
		return CatalogPointer{}, nil, nil, newOperationError("catalog_fetch_failed", "获取漫画源配置失败", err)
	}
	return pointer, body, index, nil
}
