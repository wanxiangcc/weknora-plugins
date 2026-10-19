# SVN（Subversion）数据源连接器

把 Subversion 仓库中选定目录下的文件同步成 WeKnora 知识文档。连接器用纯 Go 实现 WebDAV 客户端，直接与 `mod_dav_svn`（Apache / VisualSVN 等）通信，**不依赖 `svn` 命令行**，因此后端镜像无需安装 subversion。

- 类型标识：`svn`
- 源码：[`internal/datasource/connector/svn/`](../internal/datasource/connector/svn/)
- 能力：增量同步、删除对账、流式 checkpoint、按路径断点续跑

## 先决条件

1. 仓库通过 **http(s) WebDAV** 暴露（`mod_dav_svn`）。`svn://`、`svn+ssh://`、`file://` 在配置期即被拒绝，并给出明确提示。
2. 同步账号至少有对目标目录的读权限（`PROPFIND` + `GET`）。
3. 如果仓库在内网，把其主机加入部署的 `SSRF_WHITELIST_EXTRA`（支持精确域名、`*.example.com`、IP、CIDR）。

## 配置

### Credentials

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `base_url` | 是 | 仓库根或任意子目录的 http(s) URL（如 `https://svn.example.com/svn/repo/trunk`）。**不要**在 URL 里内嵌账号密码。连接测试会对它做一次 `Depth: 0` 的 `PROPFIND`。 |
| `username` | 否 | 基础认证用户名。 |
| `password` | 否 | 基础认证密码；填写密码时必须同时提供用户名。 |

### Settings

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `repositories` | 空（同步 `base_url`） | 仓库数组，每项 `{ "url": "...", "paths": ["dir/a", "dir/b"] }`。`paths` 为空表示整个仓库；重复 URL 会合并路径；嵌套路径自动折叠。 |
| `max_file_size_mb` | 空（用部署上限） | 单文件大小上限（MB）。只会**收窄**部署的 `MAX_FILE_SIZE_MB`，不会突破；超过时按跳过处理并记录原因。 |

> `max_file_size_mb` 小于部署上限才有意义；如果填得比部署上限大，会打 WARN 并按部署上限执行。

### 资源选择

- 第 1 层列出 `settings.repositories`（未配置时是 `base_url` 本身）。
- 展开某个仓库后，按层列出其下的目录（只返回目录，不返回文件）。
- 选中一个目录 = 同步该目录下所有受支持的文件；选择是"包含式"的，不提供逐文件勾选。

## 同步语义

### 增量

每次同步遍历选定子树一次，用每个条目的 `DAV:version-name`、ETag、MD5、大小与游标里的快照比对，只下载发生变化的文件。不解析 `svn log`，所以不需要仓库的日志权限，也不受服务端日志保留策略影响。

### 断点与超时

- 每个**路径选择**完成即写一次 checkpoint；任务因 Asynq 超时或服务重启中断后，下一次从下一个未完成路径继续，不重复下载已完成的路径。
- 一个仓库的删除基线（`Repositories`）只在仓库整体完成后才提交，未完成的部分放在 `Pending`，避免半途的"没看到"被误判为删除。
- 单次 `PROPFIND` 超时 120s；单文件下载超时按大小阶梯放宽（2 分钟起，最多 30 分钟）。

### 失败处理

| 情况 | 行为 |
| --- | --- |
| 单文件下载失败（连接重置、EOF、上游 5xx） | 只影响该文件：记录失败条目并继续；已同步过的旧版本保留，不会被删除对账误删。 |
| 文件超过大小上限 | 预检跳过（不下载），报告为失败条目并说明上限；提高上限后会自动重新评估。 |
| 配置的目录不存在 | 报告一次并继续；该子树内已同步的文档保留，不做删除。 |
| 凭据失效 / 上下文取消 | 整次同步失败（继续重试也没有意义）。 |
| 目录遍历过深 / 节点过多 | 深度上限 64、单仓库节点上限 200000，超出后提示缩小选择范围。 |

### 删除对账

仓库完整遍历后，游标里有、这次遍历没看到的文件被视为已从源端删除。某棵子树本次读取失败时，它下面的条目会被原样带进新快照（"不知道"不等于"已删除"），避免一次权限或网络抖动清空目录。

### 支持的格式

仅同步知识导入管线能处理的扩展名：

```text
.pdf .txt .docx .doc .epub .html .htm .mhtml .md .markdown .mdx
.csv .xlsx .xls .pptx .ppt .json
```

图片与音频**默认不同步**：WeKnora 只有在知识库配置了 VLM / ASR 模型时才导入这两类文件，否则会在入库前拒收。提前同步只会下载整个仓库的图片和录音再丢弃，并把这些路径写进快照，导致以后配置了模型也不会重试。需要时可在 `isSupportedFile` 中与模型一起放开。

## 排障

| 现象 | 原因与处理 |
| --- | --- |
| `did not accept WebDAV PROPFIND` | URL 不是由 `mod_dav_svn` 提供，或用了 `svn://`；换成 http(s) 的 WebDAV 地址。 |
| `401/403` | 账号无读权限，或密码错误；确认 `username`/`password`。 |
| `SSRF validation failed` / `resolves to restricted IP` | 内网主机被 SSRF 策略拦截；加入 `SSRF_WHITELIST_EXTRA`。 |
| 同步一直 running | 大量小文件或大文件导致单任务超时；一次选少一点目录，或调大 `DATASOURCE_SYNC_TIMEOUT`（如部署暴露该配置）。断点会保留，下一次继续。 |
| 某些文件没有出现在知识库 | 检查扩展名是否在支持列表；检查是否因大小上限被跳过（同步日志会写明）。 |

## 安全

- 凭据只从数据源配置读取，由 WeKnora 加密存储；连接器不落盘、不写日志。
- URL 不允许内嵌 `user:pass@`，避免凭据进入 URL、日志和快照。
- 资源列表只允许回显该数据源已配置的仓库 URL，防止列表接口变成 SSRF 探测入口。
