# 变更记录

记录 venera-server 的已发布版本。Git 标签形如 `v0.1.0`，与 Docker 镜像
`yuxuanmian/venera-server` 的同名标签及 `latest` 一一对应（`edge` 跟随 master，不入本表）。

## v0.1.0 (2026-09-13)

首个版本：收敛为 Catalog 发布服务（ADR-0013，见工作区 `doc/design/adr/`），并补上容器化部署
与镜像发布流水线。

### 新增

- Catalog 发布服务与 HTTP 接口：
  - `GET /api/health`、`GET /api/catalog/authority`
  - `GET /admin/api/catalog/status`、`POST /admin/api/catalog/check`、`POST /admin/api/catalog/activate`
- 人工发布流程：Check 只下载、校验并缓存完整 index；Activate 才写入 `data/catalog/state.json`
  的 active/history。重启只恢复本地状态，不会把远端新内容悄悄发布；历史版本可从管理页回滚，
  回滚不重新访问远端。
- 内嵌单页管理界面 `/admin/`：程序内嵌静态资源，不打包 Node、无 CDN 依赖，可离线打开。
- 静态配置 + 环境变量覆盖（非空环境变量优先）：
  `VENERA_CONFIG_FILE`、`VENERA_ADDR`、`VENERA_DATA_DIR`、`VENERA_CATALOG_URL`；
  `catalog_url` 必须形如 `raw.githubusercontent.com/<owner>/<repo>/<ref>/index.json`，
  非完整 SHA 的 ref 会在 Check 时解析为完整 SHA。
- 容器镜像：多阶段构建到 scratch（最终仅含静态二进制与 CA 证书，约 3 MB），
  同时提供 `linux/amd64` 与 `linux/arm64`。
- 两份 Compose：`compose.yml`（纯环境变量启动，不需要 config.json）与
  `compose.config.yml`（只读挂载 JSON）。
- 镜像发布流水线：推送到 master 产出 `edge` + `sha-<短SHA>`；推送 `v*` 标签产出租语义化版本、
  `<major>.<minor>` 与 `latest`。

### 变更

- 服务收敛为只负责 Catalog 发布，不再需要 SQLite、扫描器、共享源账号或 GitHub token。
- `data_dir` 相对路径按配置文件所在目录解析，服务端 Catalog 状态独立保存在 `data/catalog/`。

### 移除

- 旧的云扫与跟踪链路、扫描/任务/注册/脚本执行接口、旧 Admin API 与旧前端工程：
  旧接口不再提供兼容层，一律返回 404。

### 安全边界

- Catalog 与 admin 接口**没有业务鉴权**（受限开发合同）。镜像与 Compose 默认只绑定
  `127.0.0.1`；需要局域网访问时必须显式改 `VENERA_BIND_IP` 并由操作者自行隔离网络，
  **不要直接暴露到公网**。
- 容器以 UID/GID 65532 运行，根文件系统只读，丢弃全部 capability，启用 `no-new-privileges`。
- 镜像内不含本机 `config.json`、`.env`、旧数据库、缓存或编译产物（`.dockerignore` 白名单）。

### 升级与部署注意

- 旧数据库与用户数据不会被服务自动删除或迁移；新环境成功前不清理旧文件。
- 既有宿主机 `data/` 不会自动迁入命名卷；改用 `./data:/data` bind mount 时需自行保证
  UID 65532 可写。
- 首次创建的命名卷会继承镜像 `/data` 的属主（65532:65532）；已存在的卷不会被重新 chown，
  权限不对时请 `docker compose down -v` 重建，不要仅凭 `up -d` 的退出码判断部署成功。
- `compose.config.yml` 的 JSON 挂载即使声明了 `create_host_path: false`，部分 Docker Desktop
  仍可能把缺失路径建成目录，使程序报 `config.json: is a directory` 并进入重启循环。
- 配置在启动时读取，修改后需 `up -d --force-recreate` 才生效。

### 验证

- `go vet ./...`、`go test ./...` 全部通过。
- 发布流水线在只读根文件系统 + 命名卷的真实容器里校验 `/api/health`、`/data` 卷属主继承
  与监听日志，全部通过后才推送镜像。

### 镜像

```sh
docker run -d --name venera-server \
  --read-only --cap-drop ALL --security-opt no-new-privileges:true \
  -p 127.0.0.1:8080:8080 \
  -e VENERA_ADDR=0.0.0.0:8080 -e VENERA_DATA_DIR=/data \
  -e VENERA_CATALOG_URL=https://raw.githubusercontent.com/yuxuanmian/venera-configs/yxm/index.json \
  -v venera-catalog-data:/data \
  yuxuanmian/venera-server:v0.1.0
```

启动后打开 `http://127.0.0.1:8080/admin/`，先“检查配置”，确认候选后“激活候选”；
仅启动容器不会自动激活 Catalog，`/api/health` 正常也不代表 Catalog 已激活。
