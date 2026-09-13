# Venera Catalog Authority Server

这是一个仅负责漫画源 Catalog 发布的本机/隔离网络 Go 服务。启动配置来自环境变量和静态
`config.json`（或 `VENERA_CONFIG_FILE` 指定的文件），非空环境变量优先：

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

## Docker Compose

需要 Docker 的 Linux 容器引擎和 Compose v2 或更新版本。在本仓库目录运行：

```sh
docker compose up -d --build
docker compose logs -f venera-server
```

默认使用 `compose.yml` 的环境变量启动，不需要 config.json。访问
`http://127.0.0.1:8080/admin/` 完成 Check、Activate；仅启动容器不会自动激活 Catalog。
`GET /api/health` 返回正常不代表 Catalog 已激活。

### 环境变量方式

可以直接编辑 compose.yml 的 `environment`，或将 `.env.example` 复制为 `.env` 后修改。
已有 `.env` 时合并需要的字段，不要覆盖原配置；也可用独立文件：

```sh
docker compose --env-file .env.example config
docker compose --env-file my-server.env up -d --build
```

| 配置 | 用途 |
| --- | --- |
| `VENERA_CATALOG_URL` | Catalog index URL，对应 JSON 的 catalog_url |
| `VENERA_BIND_IP` | 宿主机绑定地址，默认 127.0.0.1 |
| `VENERA_PORT` | 宿主机端口，默认 8080 |
| `VENERA_ADDR` | 程序监听地址；Compose 固定为容器内 0.0.0.0:8080 |
| `VENERA_DATA_DIR` | 程序数据目录；Compose 固定为容器内 /data |

`VENERA_BIND_IP` 和 `VENERA_PORT` 是 Compose 端口变量，不是 Go 配置字段；修改宿主机端口无需修改容器监听。
`.env` 用于 Compose 替换 `${...}`，并不会自动把其中所有变量传入容器。
需要给局域网手机访问时，将 VENERA_BIND_IP 改为宿主机的局域网地址或 0.0.0.0，
并保持隔离网络：本服务包括管理接口，当前没有业务鉴权，不应直接暴露到公网。

### JSON 文件方式

另提供独立的 `compose.config.yml`，只读挂载 JSON，默认使用 config.example.json：

```sh
test -f ./config.example.json && docker compose -f compose.config.yml up -d --build
```

可以在 `.env` 中设置 `VENERA_CONFIG_PATH=./config.json` 指向自己的文件。
启动前检查所选路径确实是文件；上面的 `test -f` 路径应与 `VENERA_CONFIG_PATH` 一致。
Windows PowerShell 使用：

```powershell
# 使用其他配置时，同时修改路径及 VENERA_CONFIG_PATH。
$env:VENERA_CONFIG_PATH = './config.example.json'
if (-not (Test-Path -LiteralPath $env:VENERA_CONFIG_PATH -PathType Leaf)) {
    throw '配置文件不存在或路径是目录'
}
docker compose -f compose.config.yml up -d --build
```

虽然挂载声明了 `create_host_path: false`，部分 Docker Desktop 环境仍可能把缺失路径
创建为目录，使程序报 `config.json: is a directory` 并进入重启循环；不要仅凭 `up -d`
退出码判断部署成功。启动后检查 `docker compose -f compose.config.yml ps`、容器日志和
`GET /api/health`。缺失或无效 JSON 不会回退到其他配置启动。
此样例从 JSON 读取 catalog_url，容器监听和数据路径由 environment 覆盖；即使 `.env`
设置了 VENERA_CATALOG_URL，此文件样例也不会传入它。要覆盖 JSON 的 URL，在该样例的
environment 中显式添加 VENERA_CATALOG_URL 即可。

两份 Compose 文件二选一使用，不要叠加 `-f`，以免环境变量模式意外覆盖文件设置。
配置在启动时读取，修改后运行同一份 Compose 的 `up -d --force-recreate` 使配置生效。

### 镜像和数据

- 多阶段构建：编译阶段使用 Go 1.26；最终 scratch 镜像仅含去除调试符号的静态 Go
  程序和 CA 证书。Admin 页面嵌入程序，不打包 Node、Go SDK、Shell 或包管理器。
- `CGO_ENABLED=0`，支持通过 Buildx 构建 linux/amd64 或 linux/arm64。
- 容器以 UID/GID 65532 运行，根文件系统只读，Catalog 状态写入命名卷 catalog-data
  挂载的 /data。首次创建的卷沿用镜像目录权限。镜像不会包含本机 config.json、.env、
  旧数据库、缓存或编译产物。
- `docker compose down` 保留数据；`down -v` 会删除数据卷，不用于普通升级。
  更换配置或重建镜像保留卷即可保留激活状态。既有宿主机 data 不会自动迁入卷。
- 如改用 `./data:/data` bind mount，需自行确保目录可由容器 UID 65532 写入。
  迁移既有数据时保留完整 data/catalog 目录，并在停服状态备份、复制和调整权限。
- scratch 没有 Shell/curl；从宿主机请求 `/api/health` 检查服务。未额外打包健康检查工具，
  避免增加镜像大小。

```sh
docker build -t venera-server:local .
docker image inspect venera-server:local --format '{{.Size}}'
# 单架构 ARM64 示例；部署机器使用默认 build 即可匹配其架构
docker buildx build --platform linux/arm64 -t venera-server:arm64 --load .
```

## 本地验证

```powershell
go test ./...
go vet ./...
go list -deps ./cmd/server
```
