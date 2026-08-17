# Venera Scan Server

> ⚠️ **自用项目 / Personal Project**
>
> 本项目主要为自己使用和实验而开发，不保证对外可用性、稳定性或兼容性；请勿直接用于生产环境或多人公开部署。

## 这是什么

Venera 追更扫描的服务端实现：负责调度、执行漫画源脚本、存储扫描结果，并提供简单管理后台。

## 快速开始

```bash
go run ./cmd/server
```

默认监听 `:8080`，数据目录为 `data/`。

## 文档

- 完整说明与操作手册：见 [`doc/venera-server-readme.md`](../doc/venera-server-readme.md)
- 设计上下文：见 [`doc/scan-server-context.md`](../doc/scan-server-context.md)
- 接口协议：见 [`doc/scan-server-protocol.md`](../doc/scan-server-protocol.md)
- 已知缺陷待修：见 [`doc/server-defects-todo.md`](../doc/server-defects-todo.md)

## 测试

```bash
go test ./...
```
