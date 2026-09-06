# Venera Catalog Authority Server

这是一个仅负责漫画源 Catalog 发布的本机/隔离网络 Go 服务。配置唯一来源是静态
`config.json`（或 `VENERA_CONFIG_FILE` 指定的文件）：

```json
{
  "addr": "127.0.0.1:8080",
  "data_dir": "data",
  "catalog_url": "https://raw.githubusercontent.com/yuxuanmian/venera-configs/yxm/index.json"
}
```

`VENERA_ADDR`、`VENERA_DATA_DIR`、`VENERA_CATALOG_URL` 等非空环境变量优先于文件字段；
相对 `data_dir` 按配置文件目录解析。`catalog_url` 必须是固定
`raw.githubusercontent.com/<owner>/<repo>/<ref>/index.json`，ref 会在 Check 时解析为
完整 SHA。服务不需要 GitHub token，也不支持 Web 配置编辑或自动发布。

## 发布流程

```powershell
go run ./cmd/server
```

打开 `http://127.0.0.1:8080/admin/`，先点击“检查配置”，确认候选后点击“激活候选”。
Check 只下载、校验并缓存完整 index；Activate 才更新 `data/catalog/state.json` 中的
active/history。重启只恢复本地状态，不会把远端新内容悄悄发布。历史版本也从管理页通过
同一个 Activate 接口回滚，回滚不重新访问远端。

接口包括：

- `GET /api/health`
- `GET /api/catalog/authority`
- `GET /admin/api/catalog/status`
- `POST /admin/api/catalog/check`，请求体 `{}`
- `POST /admin/api/catalog/activate`，请求体 `{"catalogId":"...","revision":"..."}`

新接口没有业务 token，默认监听 localhost；如果要监听局域网，必须由操作者自行隔离网络。
旧 `/api/tracking/`、扫描、任务、注册及旧 Admin API 已移除并返回 404。旧数据库和用户数据
不会被服务自动删除或迁移；服务端 Catalog 状态独立保存在 `data/catalog/`。

## 验证

```powershell
go test ./...
go vet ./...
go list -deps ./cmd/server
```
