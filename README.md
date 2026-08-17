# Venera Scan Server — PoC

本目录用于 Venera 追更扫描服务端的 PoC。

## 状态

| 步骤 | 状态 |
|---|---|
| QuickJS 语法/特性冒烟（真实脚本） | ✅ 34/34 通过 |
| Go 宿主桥接（sendMessage） | ✅ 公共 jsbridge：http/convert/html/cookie/random/uuid 等 |
| 双端对照夹具（录制-回放） | ✅ mock + MangaDex + Ehentai 真实源双端 diff 通过 |
| 服务端 engine 真实夹具回放 | ✅ MangaDex / Ehentai `engine.RunJob` 回放通过 |
| 真实源端到端（真实 cookie/网络） | ✅ Ehentai 使用本机已有 Cookie 录制跑通；更多源待补 |

## 环境

- 本机 Go 1.26 + MinGW gcc（`D:\env\winlib\mingw64\bin\gcc.exe`）可运行 CGo/quickjs-go。
- 不再依赖 Docker/WSL。

## 快速开始

### 1. 全量脚本语法/特性冒烟

```bash
# 在 venera-server 目录下，用 Docker 运行
docker run --rm \
  -v "D:\project\tool\venera-project:/workspace" \
  -w /workspace/venera-server \
  golang:1.26 \
  sh -c 'go run ./cmd/poc-smoke /workspace/venera/assets/init.js $(find /workspace/venera-configs -maxdepth 1 -name "*.js" ! -name "_template_.js" ! -name "_venera_.js" | sort)'
```

预期：全部 `SCRIPT OK`，最后 `SMOKE PASS`。

### 2. 宿主桥接最小验证

```bash
docker run --rm \
  -v "D:\project\tool\venera-project:/workspace" \
  -w /workspace/venera-server \
  golang:1.26 \
  sh -c 'go run ./cmd/poc-call /workspace/venera/assets/init.js /workspace/venera-server/pocdata/test_source.js TestSource 42 /workspace/venera-server/pocdata/comic_42.json'
```

预期：输出 JSON 字符串，包含 `id`, `title`, `cover`, `updateTime`, `chapters`。

## 已验证结论

- `assets/init.js` 可在 quickjs-go 中加载。
- 真实漫画源脚本（`venera-configs/*.js`）在独立 QuickJS 上下文中均可加载，无语法/特性阻断。
- Go 侧 `sendMessage` 桥接可处理 `http` 消息并返回 JSON，JS 的 `loadInfo` 可正常 `await` 并返回结果。
- 早期遇到的退出崩溃已定位为函数值重复 `Free()` 导致，修正后无崩溃。

## 真实源端到端（poc-real）

`cmd/poc-real` 会真实发起 HTTP 请求，并把请求/响应录制为 JSONL。

### 步骤

1. 在一台**能联网**的机器上启动 Docker（当前环境已验证 Docker 可用）。
2. 从**无需登录的公开源**开始，例如 MangaDex：
   - 打开 MangaDex 任意漫画页，复制 URL 中的 UUID 作为 `<COMIC_ID>`。
3. 运行：
   ```bash
   docker run --rm \
     -v "D:\project\tool\venera-project:/workspace" \
     -w /workspace/venera-server \
     golang:1.26 \
     sh -c 'go run ./cmd/poc-real \
       /workspace/venera/assets/init.js \
       /workspace/venera-configs/manga_dex.js \
       MangaDex \
       <COMIC_ID> \
       /workspace/venera-server/pocdata/mangadex_<COMIC_ID>.jsonl'
   ```
4. 如果源需要登录，把 Cookie 作为第 7 个参数传入：
   ```bash
   sh -c 'go run ./cmd/poc-real \
     /workspace/venera/assets/init.js \
     /workspace/venera-configs/xxx.js \
     Xxx \
     <COMIC_ID> \
     /workspace/venera-server/pocdata/xxx_<COMIC_ID>.jsonl \
     "session=...; other=..."'
   ```
5. 命令会：
   - 打印 `loadInfo` 返回的 JSON；
   - 把每次 HTTP 请求/响应写入指定的 `.jsonl` 文件。
6. 录制的 JSONL 就是后续双端回放夹具的素材。

> ⚠️ JSONL 可能包含 Cookie 等敏感信息，入库/提交前必须清洗。

## 双端 diff（mock + MangaDex + Ehentai 真实源）

已跑通：
- **mock**：Dart `poc record` 录制 → Go/Dart 回放一致。
- **MangaDex 真实源**：Dart 录制（App 代理/Cookie 链）→ Go/Dart 回放一致。
- **Ehentai 真实源（已有 Cookie）**：Dart 录制（使用本机 Venera Cookie）→ Go/Dart 回放一致。

### mock 步骤

1. 启动本地 mock 服务器（后台）：
   ```powershell
   cd D:\project\tool\venera-project\venera-server
   go run ./cmd/mock-server
   ```
2. Dart 侧录制：
   ```powershell
   cd D:\project\tool\venera-project\venera
   flutter run -d windows -a --headless -a poc -a record -a D:/project/tool/venera-project/venera-server/pocdata/mock_source.js -a 42 -a D:/project/tool/venera-project/venera-server/pocdata/mock_42.jsonl
   ```
3. Go 侧回放：
   ```powershell
   cd D:\project\tool\venera-project\venera-server
   go run ./cmd/poc-replay D:/project/tool/venera-project/venera/assets/init.js D:/project/tool/venera-project/venera-server/pocdata/mock_source.js MockSource 42 D:/project/tool/venera-project/venera-server/pocdata/mock_42.jsonl
   ```
4. Dart 侧回放：
   ```powershell
   cd D:\project\tool\venera-project\venera
   flutter run -d windows -a --headless -a poc -a replay -a D:/project/tool/venera-project/venera-server/pocdata/mock_source.js -a 42 -a D:/project/tool/venera-project/venera-server/pocdata/mock_42.jsonl
   ```

### MangaDex 真实源步骤

1. Dart 侧录制：
   ```powershell
   cd D:\project\tool\venera-project\venera
   flutter run -d windows -a --headless -a poc -a record -a D:/project/tool/venera-project/venera-configs/manga_dex.js -a 3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c -a D:/project/tool/venera-project/venera-server/pocdata/mangadex_3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c.jsonl
   ```
2. Go 侧回放：
   ```powershell
   cd D:\project\tool\venera-project\venera-server
   go run ./cmd/poc-replay D:/project/tool/venera-project/venera/assets/init.js D:/project/tool/venera-project/venera-configs/manga_dex.js MangaDex 3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c D:/project/tool/venera-project/venera-server/pocdata/mangadex_3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c.jsonl
   ```
3. Dart 侧回放：
   ```powershell
   cd D:\project\tool\venera-project\venera
   flutter run -d windows -a --headless -a poc -a replay -a D:/project/tool/venera-project/venera-configs/manga_dex.js -a 3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c -a D:/project/tool/venera-project/venera-server/pocdata/mangadex_3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c.jsonl
   ```

### Ehentai 真实源步骤（已有 Cookie）

1. Dart 侧录制（自动使用本机 Venera Cookie）：
   ```powershell
   cd D:\project\tool\venera-project\venera
   flutter run -d windows -a --headless -a poc -a record -a D:/project/tool/venera-project/venera-configs/ehentai.js -a "https://exhentai.org/g/3444864/94f80c44f8/" -a D:/project/tool/venera-project/venera-server/pocdata/ehentai_3444864.jsonl
   ```
2. Go 侧回放：
   ```powershell
   cd D:\project\tool\venera-project\venera-server
   go run ./cmd/poc-replay D:/project/tool/venera-project/venera/assets/init.js D:/project/tool/venera-project/venera-configs/ehentai.js Ehentai "https://exhentai.org/g/3444864/94f80c44f8/" D:/project/tool/venera-project/venera-server/pocdata/ehentai_3444864.jsonl
   ```
3. Dart 侧回放：
   ```powershell
   cd D:\project\tool\venera-project\venera
   flutter run -d windows -a --headless -a poc -a replay -a D:/project/tool/venera-project/venera-configs/ehentai.js -a "https://exhentai.org/g/3444864/94f80c44f8/" -a D:/project/tool/venera-project/venera-server/pocdata/ehentai_3444864.jsonl
   ```

### 服务端 engine 回放

已用真实夹具验证 `engine.RunJob`：

```powershell
cd D:\project\tool\venera-project\venera-server
go test ./internal/engine -run 'TestEngineRunJob(MangaDex|Ehentai)Replay' -v
```

## 下一步

1. 把本机 Venera 已有 Cookie/登录态导入服务端 `source_sessions`（加密落盘），让 engine 真机跑 Ehentai 等登录源。
2. 继续补 `save_data`/`load_data`、`gbk`、AES 等宿主能力。
3. 继续补其他真实源夹具。
