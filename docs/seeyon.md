# 致远OA（Seeyon V5 CAP4 报表）数据源连接器

把致远 V5 CAP4 的**查询报表**与**单表统计报表**数据同步成 WeKnora 知识文档。

- 类型标识：`seeyon`
- 源码：[`internal/datasource/connector/seeyon/`](../internal/datasource/connector/seeyon/)
- 能力：增量同步、删除对账、流式 checkpoint、资源搜索、字段选择

## 设计要点

连接器**不直连 OA 数据库**，而是通过 V5 的 **Seeyon MCP 网关**（无状态 streamable-HTTP JSON-RPC 2.0 端点）读取报表运行时数据。原因是报表的鉴权、字段级/行级数据权限与分页规则都由服务端 `report-runtime` 判定；绕过网关直接查库会绕过这些规则。网关正好是 V5 对外的官方通道。

网关把工具以不透明 ID 暴露（32 位十六进制，或 `sy-`/`cs-` + MD5），所以连接器**不硬编码工具名**：运行时从 `tools/list` 的描述解析出「报表列表 / 查询数据 / 统计数据」三个角色，也允许用 `settings.tool_overrides` 显式钉住。

**一个数据源 = 一张报表**。报表是几十行到几十万行的列表型数据，把多张表塞进一个数据源会让「同步了多少、截断到什么程度、字段属于哪张表」全部含混。因此创建流程是：

```text
选报表（关键字搜索，单选）→ 勾选字段（默认全选）→（可选）固定筛选条件 → 同步策略
```

## 先决条件

1. **MCP 个人授权码**：登录 V5 →「个人设置 → 任务令牌」→ 取 MCP 个人授权码（形如 `<随机串>.<用户ID>`）。默认 30 天有效，**不会自动续期**，过期后重新获取并更新数据源凭据。
2. **网关侧开通报表工具授权**：网关只把当前应用已授权的工具放进 `tools/list`。若返回 `工具未被允许调用: <tool-id>`，说明网关应用没有授权「报表管理」工具组，需要 OA 管理员开通；这不是连接器或凭据问题。
3. **SSRF 白名单**：若网关域名解析到内网地址，会被 WeKnora 的 SSRF 策略拦截。把网关主机加入 `SSRF_WHITELIST_EXTRA`（支持精确域名、`*.example.com`、IP、CIDR）。

## 配置

### Credentials

| 字段 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `base_url` | **是** | 无 | MCP 网关端点，取 V5 中可复制的 `mcpServers` 配置里的 `url`，形如 `https://<gateway-host>/mcp-gateway/<application-id>/apps/seeyon/message`。本仓库**不内置默认地址**。 |
| `access_token` | **是** | 无 | MCP 个人授权码，加密存储，以 `Authorization: Bearer` 发送。 |

### 资源选择

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `resource_ids` | 是 | **恰好一张**报表的 ID。空选择会被拒绝；多选也会被拒绝。 |

报表选择器只按关键字搜索（`keyword`），不会一次性拉取全部报表：租户可能暴露上千张报表，完整列表在网关侧已经出现过 504。搜索为空时不返回任何资源，"搜索全部"正是这个接口要避免的事。

选中报表后进入「选择字段」步骤：连接器以该报表为 parent 解析字段，查询报表直接读定义里的 `columnIds`；单表统计没有 `columnIds`，会探测一行数据，从 `drillContext.fieldValues` 还原列 ID。

### Settings

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `fields` | 空（全部列） | 要同步的字段 ID 列表，格式 `<表名>_<字段名>`。保存后报表在源头改了列（删除/改名），已不存在的字段会被忽略并 WARN；若一个都不匹配则回退为同步全部列。 |
| `conditions` | 空（不过滤） | 固定筛选条件，原样透传给每一页请求。详见下节。 |
| `design_types` | `["QUERY","STATISTICS"]` | 允许列出的报表类型。其它类型（`MULTSTATS`、`SCHEDULEDSTATS`、`WEASTATS`、`WEAQUERY`、`SUMMARY`）在配置期被明确拒绝。 |
| `page_size` | `500` | 每页行数，服务端硬上限 500，超限自动夹紧。 |
| `max_document_bytes` | `1048576`（1 MiB） | 单篇文档字节上限，范围 64 KiB–16 MiB。决定一张报表被拆成几篇文档。 |
| `max_rows` | `50000` | 单次同步行数上限；`0` 表示不限；上限 500000。达到上限时停止翻页并在概览文档中标注「数据已截断」。 |
| `document_granularity` | `part` | `part` = 按字节分片（一篇文档多行）；`row` = 一行一篇（可精确引用单条记录，但行数超过 50000 会被拒绝）。 |
| `probe_incremental` | `true` | 变更探测：先用两个只取一行的请求判断数据是否变化；未变化则整轮跳过读取。 |
| `full_resync_hours` | `24` | 最多多少小时强制全量重建，兜底「来源没有维护修改时间」的情况。 |
| `timestamp_timezone` | `Asia/Shanghai` | 报表时间戳所属时区（IANA 名称）。水位按该时区渲染，不依赖同步主机本地时区。 |
| `tool_overrides` | 空 | 显式指定工具名，键为 `reports` / `query` / `statistics`。 |

前端「同步设置」暴露了 `max_document_bytes`、`max_rows`、`document_granularity`、`probe_incremental`、`full_resync_hours`、`timestamp_timezone`；`design_types`、`page_size`、`tool_overrides` 属运维级开关，需要时直接改配置。

#### 固定筛选条件

`conditions` 是数组，每项形如：

```json
{ "tableName": "formmain_1_0", "fieldName": "field1205", "op": "Equal", "value": "研发" }
```

- 条件在**每一页请求**上都会带上，增量同步拿到的始终是同一片切片。
- 操作符取报表运行时词表：`Equal`、`NotEqual`、`Like`、`NotLike`、`Include`、`In`、`IsNotNull`、`Less`、`Greater`、`LessEqual`、`GreatEqual`（注意是 `GreatEqual` 而非 `GreaterEqual`）。
- 需要 `value` 的操作符缺少值时在配置期报错。

## 同步语义

### 文档结构

- 每张报表先生成一篇**概览文档**（字段数、总行数/截断状态、分片清单、同步水位），每轮必发。
- 数据行按 `max_document_bytes` 切成若干分片文档（`part` 模式），或用一行一篇（`row` 模式）。
- 每篇文档的 `ExternalID` 由报表 ID + 分片序号/行 token 构成，稳定可复现。

### 增量

分片内容做 SHA-256 指纹，逐片与游标比对；未变化的分片不重复入库。`probe_incremental` 开启时，先发两个只取一行的请求（总数 + 顶部时间戳）判断整张报表是否变化，未变化就跳过整轮读取；`full_resync_hours` 到期后强制全量重建，兜底来源未维护修改时间的情况。

### 删除与收缩

使用 `ReplacesSubtree` + `SubtreeKeep` 声明本次同步覆盖的文档范围：报表变小、分片数减少时，多出来的旧分片会被自动清理，不会残留在知识库里。

### 失败处理

- 单张报表失败 → `PartialFetchError`，该报表沿用旧游标，下一次重试；不影响同数据源里其它报表（当前模型下一个数据源只有一张报表，但仍保留该语义）。
- 每张报表完成后 checkpoint，任务超时后续跑不必从头开始。
- 单次请求 60 秒超时；分页串行，避免并发压测网关。
- `max_rows` 达到上限是**显式截断**：停止翻页并在概览文档中标注，不会静默丢数据。

## 排障

| 现象 | 原因与处理 |
| --- | --- |
| `401/403` 或授权码无效 | 授权码过期（30 天）或被重置；重新生成并更新凭据。 |
| `工具未被允许调用: <tool-id>` | 网关应用未授权「报表管理」工具组；OA 管理员开通。 |
| 报表列表 504 / 超时 | 不要拉全量列表，使用关键字搜索；确认网关到报表运行时的链路正常。 |
| `SSRF validation failed` / `resolves to restricted IP` | 网关解析到内网地址；加入 `SSRF_WHITELIST_EXTRA`。 |
| `credentials.base_url is required` | 本仓库不内置默认网关；按 V5 的 `mcpServers` 配置填写 `base_url`。 |
| 字段一个都没匹配 | 报表列被改名/删除；重新在「选择字段」步骤勾选，或清空 `fields` 同步全部列。 |
| 概览文档显示「数据已截断」 | 达到 `max_rows`；加 `conditions` 缩小范围或调大上限（先评估网关与入库压力）。 |
| 同步很慢 | 大报表分页串行读取；可开启 `probe_incremental`（默认开）或加固定筛选。 |

## 安全

- 授权码等价于该 V5 用户的报表读取权限，按凭据管理：不要写进文档、脚本或 Issue，不要提交到仓库。
- WeKnora 对数据源凭据加密存储；连接器不落盘、不打印 token。
- 连接器不内置任何网关地址或租户标识；`base_url` 必须由部署方显式配置。
- 报表数据可能包含业务敏感字段：用 `fields` 只同步需要的列，用 `conditions` 限定行范围，按最小必要原则配置。

## 源码地图

| 文件 | 职责 |
| --- | --- |
| `types.go` | 配置解析、默认值、报表/字段模型、常量 |
| `client.go` | MCP JSON-RPC 客户端、工具解析、报表列表/数据调用 |
| `connector.go` | 连接器主流程：校验、资源、报表遍历、增量探测、checkpoint |
| `render.go` | 行数据 → Markdown 文档、分片 |
| `report.go` | 报表定义/字段/筛选条件模型 |
| `*_test.go` | 假网关端到端测试（`fake_gateway_test.go` 为网关桩） |
