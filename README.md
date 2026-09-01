# Labflow

Labflow 是一个以工件 DAG 驱动 Agent 迭代的实验运行器。一个项目目录对应一个执行实例：
项目文件始终留在原地，OpenCode Agent 直接在项目目录中工作，Labflow 只在项目内维护一个
被 Git 忽略的执行控制面。

当前设计有三个明确边界：

- **项目资产面**：`labflow-plan.toml`、目标文档、源码、数据和其他业务资产，全部位于项目目录。
- **执行控制面**：项目内的 `.labflow-exec/`，保存配置、工件标记、状态库、事件库和生成的 OpenCode 配置。
- **实验室服务目录**：首次准备时创建的 `/tmp/labflow-*`，没有名字，只记录端口等实验室级服务信息。

Labflow 不复制项目、不创建工作副本，也不提供备份或恢复层。OpenCode 和 Supervisor 都是由
操作者拥有的外部进程；Host 通过项目文件和工件标记观察、控制执行，不直接调用 OpenCode。

## 快速开始

项目根目录必须包含 `labflow-plan.toml`。初始化时只需要选择一个 OpenCode 端口：

```bash
cd /path/to/project
labflow init --port 4199
./.labflow-exec/bin/serve
```

`init` 会校验 Plan，并生成三个脚本：

```text
.labflow-exec/bin/serve
.labflow-exec/bin/attach
.labflow-exec/bin/control
```

它还会把 `.labflow-exec/` 写入当前 Git 仓库的 `.git/info/exclude`，不会修改项目的
`.gitignore`。

`serve` 会先准备执行控制面，然后在项目根目录启动 OpenCode。此时 Supervisor 尚未运行，
调度也尚未激活。另开一个终端，先发布必要的 Host 工件，再启动 Supervisor 和 Plan：

```bash
# 先复制或原子替换 Host 资产，然后发布对应工件
touch .labflow-exec/artifacts/<artifact>

# 启动 Supervisor 循环，并加载/激活 Plan
./.labflow-exec/bin/control supervisor-on
./.labflow-exec/bin/control active-on
```

从另一个终端连接 OpenCode TUI：

```bash
./.labflow-exec/bin/attach
```

查看控制状态：

```bash
./.labflow-exec/bin/control status
# active=on supervisor=on system-blocked=off
```

直接 `touch` 或删除保留标记与调用 `control` 等价：

| 操作 | 文件操作 | 含义 |
| --- | --- | --- |
| `supervisor-on` | touch `_supervisor` | 启动 Supervisor；已经运行时会重启一代 |
| `supervisor-off` | 删除 `_supervisor` | 让当前 Supervisor 立即退出，并停止后续启动 |
| `active-on` | touch `_active` | 重新读取 Plan、应用新 DAG 并允许调度 |
| `active-off` | 删除 `_active` | 停止新调度，但保留最后一次有效 Plan 和会话 |

这些文件都位于 `.labflow-exec/artifacts/`。`serve` 本身只负责 OpenCode 进程和一个简单的
Supervisor 启动循环，不解释 DAG，也不依赖 Supervisor 的内存状态。退出 `serve` 会终止它
启动的 OpenCode 与 Supervisor 子进程。

## 目录模型

```text
prj-home/
  labflow-plan.toml
  goals/
  <源码、数据和其他 Assets>
  .labflow-exec/
    bin/
      serve
      attach
      control
    config.json
    runtime.json
    host-tasks.json
    supervisor-status.json
    states.sqlite
    events.sqlite
    report-cursor
    lock
    artifacts/
      _active
      _supervisor
      _system-blocked
      <artifact>
    ws/
      opencode.json
      .opencode/
        agents/
          lab-ob.md
          <role>.md
```

`.labflow-exec/` 是临时执行状态，不允许出现在 Plan 的 `goal`、`inputs`、`assets`、
`check` 或角色权限中。业务资产仍在项目目录，OpenCode 请求的工作目录也始终是
`prj-home`；生成配置通过环境变量从控制面加载：

```text
OPENCODE_CONFIG=<prj-home>/.labflow-exec/ws/opencode.json
OPENCODE_CONFIG_DIR=<prj-home>/.labflow-exec/ws/.opencode
```

主要控制面文件：

- `config.json`：执行 ID、项目路径、Plan 路径、实验室服务目录和端口。
- `runtime.json`：最后一次成功激活的规范化 DAG 与角色配置。
- `states.sqlite`：可恢复的可变状态，包括根会话身份、任务记录和 reducer 状态。
- `events.sqlite`：摘要事件流；主表为 `timeline`，写入路径位于 `action_paths`。
- `host-tasks.json`：当前等待 Host 发布的必选与可选工件。
- `supervisor-status.json`：Supervisor 的当前调度、任务、会话和错误投影。
- `report-cursor`：Supervisor 终端进度报告已经提交的 timeline 行游标。

首次准备执行时，Labflow 还会创建 `/tmp/labflow-*`，其中的 `config.json` 只保存该
实验室服务目录、端口和 schema。项目的 `.labflow-exec/config.json` 指向它。确认 OpenCode
已经停止后，可以回收该目录：

```bash
labflow lab remove /tmp/labflow-...
```

## 执行身份

执行 ID 完全由项目目录决定：

```text
basename(realpath(prj-home)) + "." + sha256(realpath(dirname(prj-home)))[:16]
```

Plan 名称、实验室名称、执行变体、工作区继承和备份级别都不参与身份计算。当前模型中，
一个项目只有一个内联执行控制面。

## Plan

下面是一个完整的最小示例：

```toml
[roles.builder]
read = ["goals/", "bin/tool", "docs/", "shared/"]
write = ["src/", "scratch/builder/"]
commands = ["bin/tool --help", "bin/tool * -C src"]

[artifacts.tool]
assets = ["bin/tool"]

[artifacts."learn.sess.builder"]
goal = "goals/learn.md"
requires = ["tool"]

[artifacts."result.builder"]
goal = "goals/build.md"
requires = ["learn.sess.builder", "feedback?"]
inputs = ["docs/"]
assets = ["src/"]
check = ["src/result.txt"]

[artifacts.feedback]
assets = ["feedback.md"]
```

### 工件与 DAG

工件名称以 `.<role>` 结尾且带有 `goal` 时，由该角色生产；其他工件由 Host 发布。
Labflow 从角色工件的后缀推断角色，因此每个推断出的角色都必须有对应的
`[roles.<role>]`。

工件字段的语义彼此独立：

- `requires` 构成工件 DAG，决定可运行性与新鲜度。依赖名末尾的 `?` 表示可选依赖：
  它不阻塞任务，但刷新后仍参与下游迭代。
- `assets` 描述该工件的输出范围。Host 先更新这些文件，再 touch 工件标记；Agent 工件由
  Supervisor 在成功结算后发布标记。
- `inputs` 决定 Agent 收到的阅读清单。省略时，它等于直接依赖工件 `assets` 的去重并集；
  显式配置（包括 `[]`）会完全替代默认值。
- `check` 是成功回复后的机械验收路径，必须处于预期状态，工件才会结算。
- `goal` 是角色任务引用的目标文档路径。Supervisor 不把目标文档原文嵌入提示词。

项目文件本身的变化不会发布工件，也不会单独触发任务。调度只依赖工件标记的存在与
mtime。任务开始时，Supervisor 会在统一提示词中列出依赖工件和实际 inputs，并标记
“已刷新”“未改变”或“尚不存在”。目录 input 必须以 `/` 结尾，提示词会递归列出其中的
具体文件；“已更新”表示文件晚于该角色上一轮任务结束时间。

名称形如 `<name>.sess.<role>` 的工件表示绑定到角色长期会话的知识资格。它只能作为同一
角色工件的必选依赖，不能声明为可选依赖。

### 稳定角色权限

角色权限是角色级稳定配置，不随每个任务动态切换：

- `read` 控制 OpenCode 的 `read`、`glob`、`grep` 和目录列举。
- `write` 控制编辑，同时隐含同路径的读取能力。
- `commands` 是 Bash 命令模式白名单，只来自角色配置，不从任务内容猜测。

三个字段都必须显式声明；没有权限时写 `[]`。Plan 激活前，Labflow 会校验该角色所有工件
的 `goal`、解析后的 `inputs` 和 `assets` 都可读，且 `assets` 都可写。以 `/` 结尾的权限
覆盖其下所有路径；`.labflow-exec/` 始终拒绝角色访问，也不能通过宽目录权限绕过。

Supervisor 为每个角色生成一个稳定的 `.labflow-exec/ws/.opencode/agents/<role>.md`，
其中只说明角色身份和权限，不包含具体任务。角色权限发生变化时，touch `_active` 会更新
DAG 和角色文件，但还必须重启 OpenCode，才能确保它重新加载 Agent 定义。DAG revision
同时覆盖规范化工件 DAG 与角色权限；真实 revision 变化会废弃旧任务，相同 Plan 的重复
加载则保留正在执行的任务。

## Supervisor

Supervisor 为每个角色维护一个长期 OpenCode Session，并只在该角色空闲且有可运行工件时
投递任务。同一角色一次只有一个活动任务。根 Session 使用只读观察员 `lab-ob`，不参与
工件生产。

内部状态机严格采用单一 FIFO event/reducer/effect 模型：文件观察、OpenCode HTTP 事件和
effect 返回统一进入同一事件队列；reducer 是唯一修改内存状态的地方。读取 Plan、HTTP
请求、SQLite/文件写入等外部工作都作为 effect 执行，需要等待结果的 effect 无论成功或
失败都会产生新 event，再进入 reducer。这样可以保证任务结束、会话消失、工件刷新和控制
文件变化按一个确定顺序归约。

Agent 结束任务时必须使用以下协议之一：

```text
已完成任务。<简短说明>
无法完成任务。<原因>
```

“已完成”只表示 Agent 主张完成。Supervisor 随后检查 `assets` 和 `check`，通过后才刷新
工件标记。机械检查失败时，下一轮提示会额外列出检查项及其状态。以下情况会触发修复轮次：

- 回复没有以上述两个前缀之一开头；
- turn 被中止或没有正常产生 `stop`；
- Agent 声称完成，但机械检查失败。

同一任务、同一失败类型在 2 分钟内连续出现 3 次时，Supervisor 写入
`artifacts/_system-blocked` 并暂停整个执行。Agent 明确回复“无法完成任务。”时会立即阻塞。
Host 修复根因后无需删除 `_system-blocked`，只需再次执行：

```bash
./.labflow-exec/bin/control active-on
```

新的 `_active` mtime 晚于阻塞标记时，表示 Host 已确认并恢复调度；保留的阻塞文件仍可作为
诊断证据。`control status` 此时显示 `system-blocked=acknowledged`。如果阻塞标记后来再次被
更新，它会重新生效。

`_active` 也是唯一的 Plan 激活边界：修改 `labflow-plan.toml` 本身不会生效，必须 touch
`_active`。Plan 无效时，Supervisor 停止调度，保留最后一次有效 runtime，并在
`supervisor-status.json` 中报告 `plan_error`；修复后再次 touch 即可重试。删除 `_active`
会停止新会话、结算和提示 effect，但 Supervisor 仍可观察已有会话。

`_supervisor` 控制 Supervisor 的一代进程。启动时会记录该文件 mtime；文件被 touch 或删除
后，这一代立即退出。`serve` 的纯 Shell 循环会在标记仍存在时启动下一代，所以 touch 表示
重启，删除表示停机。

## Host 操作与观察

Host 不需要 OpenCode 权限。它通过文件发布资产和工件，通过只读投影观察系统：

```bash
labflow host status
labflow host pull
labflow host pull --timeout 10
labflow query 'SELECT type, COUNT(*) FROM timeline GROUP BY type'
```

`host pull` 最多等待 60 秒，一旦出现必选或可选 Host 工件请求就返回。Host 处理请求时，
先整合或更新对应业务资产，再 touch `.labflow-exec/artifacts/<artifact>`；单独修改 Asset
不会发布工件。

`labflow query` 以 SQLite `mode=ro` 打开 `events.sqlite`，并将 `states.sqlite` 以
`states` schema 附加。它还启用 `query_only` 和 authorizer 禁写，单次最多运行 2 秒、返回
1000 行，SQL 最长 10000 字符。常用表包括：

```text
timeline
action_paths
states.state
states.task_records
```

Supervisor 会将 `task_started`、`task_completed` 和 Host 等待/处理事件持续打印到 `serve`
终端，格式为 `[yy-mm-dd HH:MM:SS] ...`。报告采用 5 秒安静窗口和 15 秒最大 debounce；
`report-cursor` 在输出成功后提交，因此 Supervisor 重启不会重复或跳过已接受的批次。

### lab-ob

OpenCode 根会话默认 Agent 为 `lab-ob`。它只拥有 `labflow host status`、只读
`labflow query` 和可选 `query-om` 权限，适合在 TUI 中按自然语言询问当前状态、任务轮数、
耗时、Token、最长思考、失败命令或其他 timeline 统计。它不能修改项目或控制面。

## OM-Labflow 查询

同时设置 `TELORA_BIN` 与 `OM_LABFLOW_PATH` 后，可以将受限领域请求降低为参数化 SQL：

```bash
TELORA_BIN=bin/telora \
OM_LABFLOW_PATH=om-labflow \
labflow query-om request.json

cat request.json | \
  TELORA_BIN=bin/telora OM_LABFLOW_PATH=om-labflow \
  labflow query-om -

TELORA_BIN=bin/telora \
OM_LABFLOW_PATH=om-labflow \
labflow query-om --explain request.json
```

两个环境变量都可以使用相对于当前目录的路径。Labflow 直接 spawn：

```text
telora -C <OM_LABFLOW_PATH> eval-with @src/bin/query:main \
  --source input=<file-or-stdin+json://>
```

它不经过 Shell，继承 stdin 与 stderr，只捕获 stdout，并要求 Telora 成功返回且结果严格为
`{"sql": ..., "bindings": [...]}`。正常模式通过与 `labflow query` 相同的只读边界执行；
`--explain` 只输出 SQL 和 bindings，不打开执行数据库。

如果 `OM_LABFLOW_PATH` 位于项目内部，生成 `lab-ob` 时还会允许它读取
`docs/DOMAIN.md` 和 `docs/QUERY-DESIGN-GUIDE.md`，使观察员先学习领域词汇，再生成请求。
要在 OpenCode 的 `lab-ob` 会话中使用这项能力，应让 `serve` 从启动时就继承两个环境变量：

```bash
TELORA_BIN=bin/telora \
OM_LABFLOW_PATH=om-labflow \
./.labflow-exec/bin/serve
```

## 手工启动

通常应使用 `serve`。需要排障时，与它等价的 OpenCode 命令是：

```bash
OPENCODE_CONFIG="$PWD/.labflow-exec/ws/opencode.json" \
OPENCODE_CONFIG_DIR="$PWD/.labflow-exec/ws/.opencode" \
opencode serve --hostname 127.0.0.1 --port 4199 --pure
```

Supervisor 的等待循环等价于：

```bash
labflow supervisor --port 4199 --prepare-only
while :; do
  while [ ! -f .labflow-exec/artifacts/_supervisor ]; do sleep 0.25; done
  labflow supervisor --port 4199
done
```

首次准备后，端口会固定在 `.labflow-exec/config.json` 中；后续可省略 `--port`。同一执行
不能同时由两个 Supervisor 持有，`lock` 会拒绝第二个实例。

## 开发与运行

要求 Python 3.11 或更高版本。项目本身没有第三方 Python 运行时依赖。

```bash
uv run python -m unittest discover -s tests -v
uv run python -m py_compile src/labflow/*.py
```

也可以直接从固定提交或不可变 release tag 运行：

```bash
uvx --from 'git+https://github.com/hh9527/labflow.git@<commit>' labflow --help
```

用于实验时应固定提交或 release tag，避免运行中的控制脚本和 Python 实现发生漂移。
