# WeKnora Plugins

面向 [WeKnora](https://github.com/Tencent/WeKnora) 的两个**独立开发**数据源连接器：**SVN（Subversion）** 与 **致远OA（Seeyon V5 CAP4 报表）**。两者都实现 WeKnora 的数据源 `datasource.Connector` 契约，接入后即可在「知识库 → 数据源」中像内置连接器一样创建、增量同步与删除对账。

> 本仓库不是 WeKnora 官方仓库，与 Tencent / WeKnora 上游无隶属关系。代码按原 WeKnora 目录结构放置，便于直接投放与打补丁。

## 包含的连接器

| 连接器 | 类型标识 | 数据来源 | 认证 | 增量/断点 | 源码 |
| --- | --- | --- | --- | --- | --- |
| SVN | `svn` | 任意经 mod_dav_svn 以 http(s) 暴露的 Subversion 仓库目录 | 可选用户名/密码 | 是（按文件 revision 快照比对 + 按路径 checkpoint） | [`internal/datasource/connector/svn/`](internal/datasource/connector/svn/) |
| 致远OA-查询/统计 | `seeyon` | 致远 V5 CAP4 查询报表 / 单表统计报表，经 Seeyon MCP 网关读取 | MCP 个人授权码（Bearer） | 是（内容指纹 + 变更探测 + 全量兜底） | [`internal/datasource/connector/seeyon/`](internal/datasource/connector/seeyon/) |

详细的配置项、同步语义与排障见：

- [docs/svn.md](docs/svn.md)
- [docs/seeyon.md](docs/seeyon.md)
- [docs/sanitization.md](docs/sanitization.md) —— 本仓库做了哪些脱敏、带来哪些行为变化

## 仓库结构

```text
.
├── internal/datasource/connector/
│   ├── svn/                  # SVN 连接器（含测试）
│   └── seeyon/               # 致远OA 连接器（含测试）
├── integration/
│   ├── apply.sh              # 一键投放：拷贝连接器 + 打核心接入补丁
│   ├── README.md             # 接入说明与手工接入点
│   └── patches/              # 针对 WeKnora v0.8.2 基线的补丁
└── docs/                     # 连接器文档与脱敏说明
```

> 本仓库是**源码投放包**，自身不含 `go.mod`：连接器按 WeKnora 原路径放置，需要接入目标仓库（或把目标仓库的依赖准备好）后才能编译与测试，接入方法见 [integration/README.md](integration/README.md)。

## 环境要求

- WeKnora **v0.8.2**（补丁基于提交 `a64645b4`；更新的上游版本可能需要手工对齐，见 [integration/README.md](integration/README.md)）
- Go **1.26+**（与 WeKnora `go.mod` 一致）
- 前端需要 Node 20+ / pnpm（仅 `0004`、`0005` 两个补丁涉及前端）
- SVN：服务端以 `mod_dav_svn`（Apache / VisualSVN）通过 http(s) 暴露仓库
- 致远OA：V5 部署已开通 MCP 网关，且网关应用授权了「报表管理」工具组；同步账号能拿到「MCP 个人授权码」

## 快速接入

```bash
git clone https://github.com/wanxiangcc/weknora-plugins.git
cd weknora-plugins

# TARGET 指向你的 WeKnora 仓库根目录（需是 git 仓库）
./integration/apply.sh /path/to/WeKnora

# 只接后端、不动前端：
./integration/apply.sh /path/to/WeKnora --backend-only
```

`apply.sh` 会：

1. 把 `internal/datasource/connector/{svn,seeyon}` 拷贝到目标仓库；
2. 按序应用 `integration/patches/*.patch`（`git apply -3`，失败时可用 `--dry-run` 预检）；
3. 打印后续自检命令。

补丁覆盖的接入点：

| 补丁 | 内容 |
| --- | --- |
| `0001-core-connector-api.patch` | 新增 `ConnectorTypeSeeyon` 常量；新增两个可选能力接口 `ResourceSearcher` / `ResourceFieldsResolver`；注册连接器元数据 |
| `0002-service-and-handler.patch` | 服务层 `SearchAvailableResources` / `ResolveResourceFields`；`GET /datasource/{id}/resources` 支持 `keyword`、`name` |
| `0003-registry.patch` | 在 `container.go` 注册连接器；登记注册表测试 |
| `0004-frontend.patch` | 数据源编辑器（报表搜索、字段选择、固定筛选、同步设置）、API 参数、6 个语言包、图标映射 |
| `0005-frontend-icon.patch` | 新增致远OA 图标资源 |

接入后自检：

```bash
cd /path/to/WeKnora
go build ./internal/...
go test ./internal/datasource/connector/svn/... ./internal/datasource/connector/seeyon/...
go test ./internal/container/ -run TestConnector
```

前端改动可跑 `pnpm --dir frontend type-check`。

## 两个数据源的关键行为

### SVN

- 纯 Go 实现 WebDAV，**不依赖 svn 命令行**；只支持 `http(s)://`（`svn://`、`svn+ssh://` 在配置期明确拒绝）。
- 一个数据源可配置多个仓库（`settings.repositories`），每个仓库可再限定目录；不配置时同步 `base_url` 指向的整个子树。
- 增量同步基于每个文件的 `DAV:version-name` / ETag / MD5 快照，不解析 `svn log`。
- 每个路径选择完成后 checkpoint，单次任务超时后可从下一个未完成路径续跑。
- 单文件下载失败只影响该文件；超过部署入库上限的文件预检跳过；配置目录不存在时报告并继续。
- 只同步知识导入管线可处理的格式（文档、表格、文本、HTML/JSON 等）；图片/音频默认不同步，避免下载后被拒收并永久留在快照里。

### 致远OA（Seeyon V5）

- 一个数据源同步**恰好一张报表**，创建流程是「选报表 → 勾字段 →（可选）配固定筛选」。
- 不直连 OA 数据库，而是走 V5 的 MCP 网关（无状态 streamable-HTTP JSON-RPC 2.0），由服务端判定报表鉴权、字段权限与分页规则。
- 报表列表支持服务端关键字搜索，避免一次拉取上千张报表；字段步骤只解析选中的那一张报表。
- 按字节上限把一张报表拆成多篇文档；也可选择「一行一篇」。增量用分片内容 SHA-256 指纹比对，未变化的报表整轮跳过，并按 `full_resync_hours` 兜底重建。
- 删除检测使用 `ReplacesSubtree` + `SubtreeKeep`：报表变小或分片减少时自动清理多余文档。

## 安全与脱敏

本仓库**不包含任何真实凭据或内部地址**：

- 致远OA 连接器**不再内置默认网关 URL**，`base_url` 为必填项；文档、前端占位符与语言包统一使用 `https://<gateway-host>/mcp-gateway/<application-id>/apps/seeyon/message` 形式的占位符。
- 已移除真实报表名、表单 ID、生产数据量、内部工单号与内部仓库地址。
- 测试数据全部为构造数据；唯一出现的“令牌”是占位常量 `TEST-MCP-AUTH-CODE`。
- 凭据由 WeKnora 加密存储；请勿把授权码写进配置文件、脚本或 Issue。

> 如果网关解析到内网地址，会被 WeKnora 的 SSRF 策略拦截；把网关主机加入 `SSRF_WHITELIST_EXTRA`（支持精确域名、`*.example.com`、IP、CIDR）。SVN 连接内网仓库同理。

## 兼容性与升级

- 补丁以 WeKnora **v0.8.2 / `a64645b4`** 为基线生成。上游若改动了同一批文件，`git apply -3` 可能产生冲突；冲突处理与手工接入点见 [integration/README.md](integration/README.md)。
- 连接器本身只依赖 `internal/datasource`、`internal/types`、`internal/logger`、`internal/utils` 的稳定符号，独立于前端与业务层，升级时通常无需改动。

## License

MIT，见 [LICENSE](LICENSE) 与 [NOTICE](NOTICE)。代码最初在一个 WeKnora 定制分支中开发，感谢 WeKnora / Tencent 的 MIT 开源工作。
