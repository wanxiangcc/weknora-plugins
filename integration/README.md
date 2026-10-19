# 接入 WeKnora

本目录把两个连接器接入一个 WeKnora 仓库：拷贝连接器源码 + 应用 5 个补丁。

- 基线：WeKnora **v0.8.2**，提交 `a64645b4`（本仓库补丁据此生成）
- 目标：`github.com/Tencent/WeKnora` 的 Go module 根目录
- 脚本不会提交、不会重启服务、不会写入任何凭据

## 一键接入

```bash
./integration/apply.sh /path/to/WeKnora            # 后端 + 前端
./integration/apply.sh /path/to/WeKnora --backend-only
./integration/apply.sh /path/to/WeKnora --dry-run   # 只预检
```

脚本行为：

1. 拷贝 `internal/datasource/connector/{svn,seeyon}`（会覆盖同名目录，脚本会先告警）；
2. 依次应用 `integration/patches/*.patch`：先 `git apply`，失败再 `git apply -3`；非 git 目录退回 `patch -p1 --fuzz=3`；
3. 打印自检与重启提示。

## 补丁清单

| 补丁 | 文件 | 作用 |
| --- | --- | --- |
| `0001-core-connector-api.patch` | `internal/types/datasource.go`、`internal/datasource/connector.go` | 新增 `ConnectorTypeSeeyon`；新增可选能力接口 `ResourceSearcher`、`ResourceFieldsResolver`；注册 `seeyon` 元数据 |
| `0002-service-and-handler.patch` | `internal/types/interfaces/datasource.go`、`internal/application/service/datasource_service.go`、`internal/handler/datasource.go` | 服务层 `SearchAvailableResources` / `ResolveResourceFields`；资源接口支持 `keyword` / `name` |
| `0003-registry.patch` | `internal/container/container.go`、`internal/container/datasource_registry_test.go` | 注册连接器 + 注册表测试 |
| `0004-frontend.patch` | `frontend/src/api/datasource/index.ts`、`DataSourceEditorDialog.vue`、`datasourceIcons.ts`、6 个语言包 | 报表搜索/字段选择/固定筛选/同步设置、资源接口参数、图标映射 |
| `0005-frontend-icon.patch` | `frontend/src/assets/img/datasource-seeyon.svg` | 新增图标 |

## 手工接入点

如果补丁因上游改动无法应用，按下面顺序手工补齐；除连接器目录外，每个点都很小。

### 1. 类型常量 `internal/types/datasource.go`

在 `ConnectorTypeSVN` / `ConnectorTypeSeafile` 附近加入：

```go
// ConnectorTypeSeeyon syncs CAP4 report data (query and statistics reports)
// from a Seeyon V5 deployment through its MCP gateway.
ConnectorTypeSeeyon = "seeyon"
```

### 2. 可选接口与元数据 `internal/datasource/connector.go`

- 新增 `ResourceSearcher`（`SearchResources(ctx, config, keyword)`）与 `ResourceFieldsResolver`（`ResolveResourceFields(ctx, config, resourceID, nameHint)`）两个接口；
- 在 `ConnectorMetadataRegistry` 中加入 `types.ConnectorTypeSeeyon` 条目（名称「致远OA-查询/统计」、`Priority: 15`、`AuthType: "token"`、能力 `incremental` + `deletion_sync`）。

> 这两个接口是**可选能力**：运行时会做类型断言，未实现的连接器继续走原来的 `ListResources` 全量列表与按名称过滤，不需要改动其它连接器。

### 3. 服务层与路由

- `internal/types/interfaces/datasource.go`：`DataSourceService` 增加上述两个方法；
- `internal/application/service/datasource_service.go`：实现它们（搜索时 `keyword` 为空直接返回空集；非搜索型连接器退回全量列表后按名称过滤）；
- `internal/handler/datasource.go`：`ListAvailableResources` 增加 `keyword`、`name` 查询参数，三种模式：
  - `parent_id` 非空 → `ResolveResourceFields`（带 `name` 提示）；
  - 否则 `keyword` 非空 → `SearchAvailableResources`；
  - 否则 → 原 `ListAvailableResources`。

### 4. 注册连接器 `internal/container/container.go`

```go
import seeyonConnector "github.com/Tencent/WeKnora/internal/datasource/connector/seeyon"

// initConnectorRegistry 内：
if err := registry.Register(seeyonConnector.NewConnector()); err != nil {
    errs = errors.Join(errs, fmt.Errorf("register seeyon connector: %w", err))
}
```

SVN 连接器若已在上游注册，无需重复。

### 5. 前端

- `frontend/src/api/datasource/index.ts`：`listResources(id, parentId?, options?: { keyword?, name? })`，用 `URLSearchParams` 拼接；
- `frontend/src/views/knowledge/settings/DataSourceEditorDialog.vue`：
  - `seeyon` 类型定义（base_url 必填、access_token 密文、6 个同步设置项）；
  - 创建步骤从「凭证 → 范围 → 策略」扩展为「凭证 → 报表 → 字段 → 策略」；
  - 报表搜索用 `listResources(id, undefined, { keyword })`，字段用 `listResources(id, reportId, { name })`；
- `frontend/src/views/knowledge/settings/datasourceIcons.ts`：映射 `seeyon` 图标；
- 语言包：`datasource.seeyon.*`、`datasource.types.seeyon`、`datasource.connector.seeyon`、`datasource.connectorDesc.seeyon`、步骤名 `datasource.steps.fields`；
- 图标：`frontend/src/assets/img/datasource-seeyon.svg`。

## 冲突处理

```bash
cd /path/to/WeKnora

# 查看哪个补丁冲突
git apply --check ../weknora-plugins/integration/patches/0004-frontend.patch

# 生成 .rej 文件，按拒绝块手工合并
git apply --reject ../weknora-plugins/integration/patches/0004-frontend.patch

# 或三路合并（需要目标仓库里有基线 blob）
git apply -3 ../weknora-plugins/integration/patches/0004-frontend.patch
```

前端补丁最容易受上游改动影响；如果 `DataSourceEditorDialog.vue` 差异过大，可只保留后端补丁（`--backend-only`），再按上面的「前端」清单手工接。

## 验证

```bash
cd /path/to/WeKnora
gofmt -l internal/datasource/connector/svn internal/datasource/connector/seeyon
go build ./internal/...
go test ./internal/datasource/connector/svn/... ./internal/datasource/connector/seeyon/...
go test ./internal/container/ -run TestConnector
pnpm --dir frontend type-check      # 若打了前端补丁
```

连接器测试使用 `httptest` 假网关 / 假 WebDAV 服务，不需要真实 OA、SVN 或外网。

## 回滚

```bash
cd /path/to/WeKnora
git checkout -- internal/types/datasource.go internal/datasource/connector.go \
  internal/types/interfaces/datasource.go internal/application/service/datasource_service.go \
  internal/handler/datasource.go internal/container/container.go \
  internal/container/datasource_registry_test.go
rm -rf internal/datasource/connector/seeyon
# SVN 目录按需恢复：git checkout -- internal/datasource/connector/svn
# 前端同理：git checkout -- frontend/src/...
rm -f frontend/src/assets/img/datasource-seeyon.svg
```

若已提交，用 `git revert` 回滚对应提交。回滚代码前先在数据源页面停用/删除 `seeyon` 类型的数据源，避免后台调度任务找不到连接器。
