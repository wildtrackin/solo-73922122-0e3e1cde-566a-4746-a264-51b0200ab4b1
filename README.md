# 检验数据最小样本缩减工具（Sample Minimizer）

把引发检验清洗故障的大 CSV 自动缩减成可复现同一异常的最小样本，交给同事复现。

- **后端**：FastAPI + DuckDB（每次试跑独立的内存数据库）
- **算法**：Zeller/Hildebrandt 的 **ddmin** 增量调试，产出 **1-最小**子集
- **交付**：可直接 `docker compose up` 启动，浏览器操作

---

## 一、它解决什么问题

给定：

1. 一个检验 CSV，列固定为 `sample_id,item,value`（均为文本），最多 **1000 行**；
2. 一条查询关系 `readings` 的**确定性 SELECT**，在完整数据上会抛异常。

工具自动删减行，直到满足：

- 保留的子集仍抛出**相同异常类 + 相同首行消息**；
- 再删掉其中**任意一行**都不能复现该错误（1-最小）；
- 下载结果保留**原表头、原行序、所有重复行**。

以下情况**不会发布**最小样本：

- 原始查询本身执行成功（提示「无需缩减」，状态 `no_error`）；
- 任一次试跑超过 **2 秒**（任务未完成，状态 `timed_out`）；
- 出现其它错误（状态 `failed`）；
- 任务被取消（状态 `canceled`，所有试跑停止后才标记，此后绝不发布结果）。

### 取消与成品发布的交接（原子发布）

成品（`minimal_sample.csv`、`result_rows`、下载资格、`succeeded` 状态）是**一次完整发布**：

1. 结果先写到任务目录下的临时文件 `minimal_sample.csv.tmp`，未提交前任何查询都看不到成品——
   非 `succeeded` 状态下 `has_result=false`、`result_rows=0`、下载接口返回 409；
2. 临时文件 `os.replace` 落正名与状态置为 `succeeded` 在**同一把任务状态锁**内完成，
   不可能出现「状态还是 running 却已有结果/可下载」的中间态；
3. 每一次终态转换都在锁内复查取消事件：取消获准后，未发布的样本（含临时文件）一律丢弃，
   任务在写入与所有试跑结束后才报 `canceled`；已经发布成功的任务结果保留、明确**不可取消**
   （再点取消返回 409 并提示原因）；
4. 慢写盘不阻塞受理：写盘发生在 worker 线程且不持有状态锁，取消接口立即 ACK（`canceling`），
   轮询/SSE/列表查询与另一窗口的任务全程可用，无需等写完。

可通过环境变量 `SAMPLE_MINIMIZER_PUBLISH_PAUSE_SECONDS` 在发布前注入可控停顿；
回归测试（`tests/test_publish_cancel.py`）则使用更确定的 `JobManager.publish_gate_factory`
事件闸门：worker 在发布前阻塞在测试持有的 `Event` 上，测试先确认 worker 已在闸门上等待，
再精确控制放行/取消，完全不依赖墙钟，稳定复现「成品发布前一刻点取消」的窗口。

---

## 二、快速开始（Docker Compose）

```bash
docker compose up --build
# 打开浏览器
http://localhost:8000
```

任务产物（原始上传 `original.csv`、查询 `query.sql`、最小样本 `minimal_sample.csv`）
持久化在命名卷 `job-data`（容器内 `/data/jobs`）。停止：

```bash
docker compose down          # 保留产物卷
docker compose down -v       # 同时删除产物卷
```

### 本地直接运行（开发）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 三、操作网页

1. 选择检验 CSV（表头必须恰好为 `sample_id,item,value`，数据行 ≤ 1000）。
2. 粘贴确定性 SELECT（只能查 `readings`；禁止多语句、写操作、随机/时钟等非确定性函数）。
3. 提交后可实时看到：阶段、已完成试跑次数、当前候选行数、每次试跑结果、异常指纹。
4. 随时点「取消任务」：请求立即受理，进入 `canceling`；**等所有在跑的试跑真正停止后**
   才变为 `canceled`，且不会出现下载入口。
5. 成功后点「下载最小样本」。两个浏览器窗口提交的任务、各自的取消互不干扰。

页面底部内置两个演示（可下载 CSV、一键填入查询）：

| 演示 | 场景 | 输入 | 期望最小样本 |
| --- | --- | --- | --- |
| `agg_divzero` | **聚合执行阶段才报错**：每行都能 CAST 成功，只有 `GROUP BY` 聚合算 `SUM(1 // CAST(value AS INTEGER))` 遇到唯一的 `0` 才抛 `InvalidInputException` | 1000 行 | 1 行 |
| `agg_cast` | **真正的双行最小依赖**：`HAVING count(*)>=2` 门控，probe 组两行同时保留才放行随后的 `CAST(MAX(value) AS INTEGER)`；删任一行该组被过滤即成功 | 1000 行 | 2 行 |

---

## 四、关键设计与需求对照

| 需求 | 实现方式 |
| --- | --- |
| 每次试跑独立输入和数据库 | 每次试跑都 `subprocess` 启动全新 `python -I` 进程，进程内新建 `duckdb.connect(":memory:")`，只装载本次子集；进程退出即销毁，无任何状态共享 |
| 限 2 秒 | 父进程按墙钟 2 秒轮询，超时即杀掉试跑进程组并判定 `timeout`，任务终止且不发布 |
| 仅相同异常类 + 首行消息算复现 | 以基线试跑的 `(异常类, 首行消息)` 为指纹，`app/minimizer.py::_matches` 要求两者**完全相等**；成功、超时、其它异常均不算复现 |
| 再删任意一行都不能复现 | ddmin 收敛后再做一次逐行删除的 1-最小性核验 |
| 保留表头/行序/重复行 | 子集用「原始行索引的有序列表」表示，按索引取行、用 `csv` 模块重写，不做基于内容的集合运算，重复行天然保留 |
| 取消立即受理、试跑全停才报已取消 | 取消事件先置位、接口立即 ACK 并进入 `canceling`（终态任务返回 409，已发布成功不可取消）；每次试跑启动前预检事件，运行中的试跑靠进程组 SIGKILL（Docker 内）终止；worker 捕获取消后在状态锁内统一走终态提交，才置 `canceled` 并清空/删除未发布结果 |
| 状态、行数、下载资格共同反映一次发布 | 成品写临时文件，`os.replace` 落正名与 `succeeded`/`result_rows`/`result_path` 在同一把状态锁内原子提交；`Job.public()` 对非 succeeded 任务一律报 `has_result=false`、`result_rows=0`，下载接口仅放行 succeeded 且文件存在的任务 |
| 慢写盘仍可取消/查询 | 渲染与写盘在 worker 线程、锁外执行；取消与查询走独立线程只短暂取锁，互不等待；`SAMPLE_MINIMIZER_PUBLISH_PAUSE_SECONDS` 可注入发布停顿，测试另有确定性的发布闸门 seam |
| 两窗口任务与取消互不干扰 | 每个任务独立线程、独立取消事件、独立状态锁、独立工作目录与产物；JobManager 仅用注册表锁保护任务表 |
| 确定性 SELECT | 上传时静态校验：单条 SELECT/WITH、禁多语句与写操作关键字、禁 `random/uuid/now/current_*` 等非确定性函数 |

### 关于强杀与受限沙箱

试跑是独立**进程组**，正常环境（含交付的 Docker 容器）下取消/超时通过
`killpg(SIGKILL)` **立即**终止在跑的 DuckDB。某些加固 CI 沙箱的 seccomp 规则
会拒绝向子进程发送任何信号（`EPERM`）；此时退化为「试跑启动前预检取消标志」：
取消后不再发起任何新试跑，已在跑的那一次自然结束后任务即标记 `canceled`，
同样绝不发布结果。依赖「立即杀死在跑进程」的测试用 `@requires_signals` 标记，
在这种沙箱自动跳过，在 Docker 内正常执行。

---

## 五、测试

```bash
pip install -r requirements.txt
python -m pytest tests/ -q
```

测试覆盖：

- CSV/ SQL 校验（表头、列数、1000 行上限、UTF-8、多语句、非确定性函数）；
- 试跑隔离、指纹确定性、空表/单行、2 秒超时、取消；
- ddmin 正确性、穷举验证 1-最小性、行序与重复行、异常类/消息不一致不算复现、
  超时与取消均不发布；
- 完整 HTTP 流程：提交→进度→下载形状、`no_error`、超时不发布、取消不发布、
  双任务互不干扰、SSE 终态、演示数据端到端；
- **取消/成品交接回归（`tests/test_publish_cancel.py`）**：用
  `SAMPLE_MINIMIZER_PUBLISH_PAUSE_SECONDS` 注入可控发布停顿，确定性覆盖
  「写盘前一刻取消→丢弃未发布样本→试跑与写入全停后才 `canceled`」、
  慢写盘期间查询/另一窗口任务不被阻塞、成功发布的原子性与不可取消、
  终态取消返回 409、非成功状态绝不展示成品/行数/下载入口。

> 当前加固沙箱禁止信号，5 个依赖真实 SIGKILL 的用例会显示 `skipped`，
> 其余用例全部通过；在 Docker 容器内全部执行。

---

## 六、目录结构

```
.
├── app/
│   ├── main.py        # FastAPI 路由、SSE、下载、演示
│   ├── jobs.py        # 任务状态机、worker 线程、取消编排、产物管理
│   ├── minimizer.py   # ddmin 算法、1-最小性核验、进度
│   ├── trial.py       # 试跑子进程管理（隔离、2 秒超时、取消）
│   ├── runner.py      # 子进程入口：新建 DuckDB、建表装载、执行 SELECT
│   ├── csvutil.py     # CSV 解析校验、子集渲染、SELECT 确定性校验
│   └── static/index.html  # 单页操作界面（原生 JS，轮询 + SSE）
├── demo_data/         # 两个内置演示（CSV + 元数据 JSON）
├── tests/             # pytest 测试套件
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
