# 脱敏说明

本仓库是 SVN 与致远OA 两个数据源连接器的**公开发布版**。原代码来自内部 WeKnora 定制分支，发布前做了以下脱敏与清理。

## 原则

- 不发布真实凭据、令牌、密码。
- 不发布内部网络地址、私有部署域名、内部 GitLab / Jira 链接。
- 不发布可识别客户或租户的报表名、表单 ID、生产数据量。
- 代码仍保持可用：被移除的只是部署相关常量，配置项本身保留，改由使用方显式填写。

## 具体改动

| 位置 | 原内容 | 公开版 | 原因 |
| --- | --- | --- | --- |
| `seeyon/types.go` | `DefaultGatewayURL` 常量，内置产品网关地址与应用 ID | **删除该常量**；`credentials.base_url` 改为**必填**，缺失时报 `credentials.base_url is required` | 网关地址属于具体部署，公开默认值等于泄露内部端点，也容易让使用方误连 |
| `seeyon/types.go` 包注释 | 真实 `mcpServers` JSON（含网关地址） | 改为 `https://<gateway-host>/mcp-gateway/<application-id>/apps/seeyon/message` 占位 | 说明保留，地址去除 |
| `seeyon/client.go` | 空 `base_url` 回退到默认网关 | 空 `base_url` 直接报配置错误 | 与上一条一致 |
| `seeyon/*_test.go` | 依赖默认网关的断言；真实报表名、表单 ID、生产行数 | 改为检查“缺少 base_url 被拒绝”/显式 loopback 网关；报表名改为「示例查询报表」，表单 ID 改为 `formmain_1_0...`，规模描述改为「数十万行」等 | 测试不依赖外网 DNS，也不携带租户信息 |
| `seeyon/*.go` 注释与测试桩 | 网关应用 ID（旧 `666` / 新 `1001`） | 改为「gateway application」，测试网关 URL 使用 `/mcp-gateway/1/...` | 应用 ID 属于部署细节 |
| 前端对话框注释 | `formmain_66717_0_field1205` | `formmain_1_0_field1205` | 同表单 ID |
| `seeyon/fake_gateway_test.go` | SSRF 白名单包含产品网关域名 | 只保留 `127.0.0.1,localhost` | 测试不需要解析真实域名 |
| 前端 `DataSourceEditorDialog.vue` | `base_url` 可选，占位符为真实网关地址 | `base_url` 必填（去掉 `optional`），占位符改为 `<gateway-host>/<application-id>` | 与后端行为一致 |
| 前端 6 个语言包 | `baseUrlHint` 写明默认地址 | 改为“必填 + 占位示例 + SSRF 提示” | 同默认地址移除 |
| `svn/*.go` 注释 | 内部工单号 `AIHUB-xx` | 移除工单号，保留技术说明 | 内部流程标识不对外 |
| 未发布 | 内部运维 SQL（含内网服务器 IP）、变更规格、agent 记忆、内部 GitLab 地址 | 不在本仓库出现 | 与连接器功能无关 |

文档（`docs/svn.md`、`docs/seeyon.md`）为公开版重写，未沿用内部文档中的生产环境统计和内部链接。

## 行为变化与升级注意

**唯一的行为变化：致远OA 数据源的 `base_url` 从“可选、有默认值”变为“必填”。**

- 新建数据源：按 V5 `mcpServers` 配置里的 `url` 填写。
- 已存在、原先依赖默认网关的数据源：升级后连接校验会失败并提示 `credentials.base_url is required`。在数据源编辑页补填网关地址后保存即可，已同步的知识不受影响。
- 其它配置项、同步语义、字段/筛选、增量与删除对账逻辑均未改变。

## 已确认不含敏感信息

- 无任何真实 `access_token` / 密码 / 私钥；测试中唯一令牌是占位常量 `TEST-MCP-AUTH-CODE`。
- 测试 URL 仅使用 `127.0.0.1`、`example.com`、`example.invalid` 与 `169.254.169.254`（SSRF 测试用的云元数据地址，属于测试意图本身）。
- 仓库以单次提交发布，不包含内部历史提交。

可自行复核：

```bash
# 内部主机名、内网 IP、内部工单号
grep -rInE 'seeyon\.com|gitlab\.seeyon|AIHUB|(^|[^0-9])10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.' .
# 疑似真实令牌
grep -rInE 'Bearer [A-Za-z0-9._-]{20,}|access_token["'\'': ]+[A-Za-z0-9._-]{20,}' .
```
