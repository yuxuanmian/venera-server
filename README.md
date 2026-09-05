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

### 本地调试：临时关闭 HTTP 鉴权

如需调试 Cloud Tracking，可复制 [`config.example.json`](config.example.json) 为
`config.json`，并保持 `debug_open_auth` 为 `true`。示例文件已经包含本地
`venera-configs` checkout 和当前 revision：

```bash
copy config.example.json config.json       # Windows
cp config.example.json config.json         # Linux/macOS
go run ./cmd/server
```

该开关启用后，所有 `/api` 和 `/admin` 鉴权都会被跳过；请求使用配置中的固定调试用户/设备，
因此客户端不需要 Access Token，`POST /api/register` 也可以不带 Bearer Token。请求参数校验、
Cloud Tracking 的 catalog/revision/runtime 校验仍然保留。它只适合本机或隔离网络调试，绝不要把
开启此开关的服务暴露到公网。环境变量 `VENERA_CONFIG_FILE` 可指定配置文件路径，且
所有 `VENERA_TRACKING_*` 与 `VENERA_DEBUG_*` 环境变量都优先于 JSON 配置。删除 `config.json` 或将开关改为
`false` 后重启服务即可恢复鉴权。

## 文档

- 完整说明与操作手册：见 [`doc/venera-server-readme.md`](../doc/venera-server-readme.md)
- 设计上下文：见 [`doc/scan-server-context.md`](../doc/scan-server-context.md)
- 接口协议：见 [`doc/scan-server-protocol.md`](../doc/scan-server-protocol.md)
- 已知缺陷待修：见 [`doc/server-defects-todo.md`](../doc/server-defects-todo.md)

## 测试

```bash
go test ./...
```

## Cloud Tracking v1

Cloud Tracking 是独立的 `/api/tracking/` API 和只读 Admin 诊断接口。Server 只激活并发布
可信目录的 `catalogId`、完整 `activeRevision`、generation 和精确 `(sourceKey, fileName)`
能力；不向客户端发布脚本或 scanner 下载地址。观测带 revision、artifact、freshness 和
`favoriteUpdate`，旧 generation 或过期结果不会进入当前快照。

启用目录可以直接在 `config.json` 中配置完整的本地 checkout 与 revision：

```json
{
  "tracking_catalog_id": "yuxuanmian/venera-configs",
  "tracking_catalog_repository": "../venera-configs",
  "tracking_revision": "<full lowercase commit SHA>",
  "tracking_cache_dir": "data/tracking-cache",
  "tracking_interval": "12h",
  "tracking_snapshot_max_requests": 64,
  "tracking_snapshot_max_items": 0,
  "tracking_snapshot_deadline": "2m",
  "tracking_max_attempts": 3,
  "tracking_observation_limit": 10000,
  "tracking_index_max_bytes": 4194304
}
```

其中 `tracking_catalog_repository` 和 `tracking_cache_dir` 的相对路径以配置文件所在目录为基准。
也可以继续使用同名的 `VENERA_TRACKING_*` 环境变量覆盖单项配置。

持久化采用 additive SQLite 表 `tracking_client_state`、`tracking_interests`、
`tracking_observations` 和 catalog 状态表；用户身份、Cookie、comic ID、marker 与 metadata
不会出现在 Admin 诊断响应中。`GET /admin/api/tracking/diagnostics` 仅接受管理员认证，
返回 active revision、客户端/interest、demand/job/checkpoint、freshness、排除原因和旧
generation 拒绝计数。

Server worker 复用了既有 QuickJS/网络边界的必要能力，但 Cloud scanner 只通过
`internal/tracking/worker` 的精确 artifact identity、来源白名单、请求/响应预算、隔离
Cookie jar 和 IPC 帧校验运行。旧 V2 prototype 仅作为只读参考，不参与启动路径或覆盖层。
