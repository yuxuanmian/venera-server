# Venera Scan Server

> **Experimental / archived**
>
> This server is an independent prototype and is not used by the Flutter
> client. It has no compatibility or production-deployment guarantee. The
> client's follow-up scan source of truth remains its local SQLite cache.

> ⚠️ **自用项目 / Personal Project**
>
> 本项目主要为自己使用和实验而开发，不保证对外可用性、稳定性或兼容性；请勿直接用于生产环境或多人公开部署。

## 这是什么

Venera 追更扫描的独立服务端原型：负责调度、执行漫画源脚本、存储扫描结果，并提供简单管理后台。

## 快速开始

```bash
go run ./cmd/server
```

默认监听 `:8080`，数据目录为 `data/`。

V2 服务入口为：

```bash
go run ./cmd/venera-server-v2
```

V2 启动前需要设置 32 字节的 `VENERA_V2_ROOT_SECRET`。也可以在服务端工作目录放置本地 `.env` 文件：

```dotenv
VENERA_V2_ROOT_SECRET=替换为32字符密钥
VENERA_V2_MANIFEST_CACHE=../venera-configs
VENERA_V2_ADDR=127.0.0.1:8080
VENERA_V2_ADMIN_DIST_DIR=web/dist
# 仅限可信开发环境：允许 App 留空登记码直接登记
# VENERA_V2_DEV_OPEN_ENROLLMENT=true
```

`.env` 只用于本地开发并被 Git 忽略；已有进程环境变量优先于 `.env`。
`VENERA_V2_DEV_OPEN_ENROLLMENT` 默认关闭。启用后，任何能访问 Server 的客户端都可以自行登记；App 仍会生成独立客户端 token，后续接口鉴权和单设备撤销保持不变。正式部署不要启用该开关。

V2 扫描状态页是本地测试用途的只读页面，不需要管理令牌，也不提供触发、重试、撤销或修改配置等操作。启动前先构建页面：

```bash
cd web
npm ci
npm test
npm run build
cd ..
go run ./cmd/venera-server-v2
```

随后打开 `http://127.0.0.1:8080/admin/`。页面的可访问范围跟随 `VENERA_V2_ADDR`；该最小版本不提供鉴权或生产部署保证。

## 文档

当前仓库未保留上述设计、协议和缺陷文档，因此不再提供失效链接。服务端代码、测试和启动方式仅作为原型参考；客户端不会调用这里的 API。

## 测试

```bash
go test ./...
cd web && npm test && npm run build
```
