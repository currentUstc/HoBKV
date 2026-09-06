# dLSM 热冷分层最终实验报告

日期：2026-09-07

## 1. 结论摘要

本报告以 30GB ZippyDB 初始化数据、16GB 独立 Put、160,000 次独立 Get 和 160,000 次混合负载为准。
node6 固定使用 16 个前台线程；node4 Server 使用与 RDMA 网卡相同 NUMA 节点上的物理核心，核心数从
2 增加到 16，步长为 2。优化版每个核心配置重复 3 次并报告均值和样本标准差；按实验约定，baseline
沿用此前每个核心配置 1 次的有效结果。

主要结果：

- 独立 Put 在全部核心配置下均提升，提升范围为 31.7%～206.6%；10 核时提升 40.1%。
- 独立 Get 在全部核心配置下均提升，提升范围为 2.5%～19.4%；10 核时提升 5.1%。
- 混合负载在全部核心配置下均为正提升，提升范围为 1.4%～49.7%；10 核时提升 15.5%。
- 4 核及以上时，优化版 node4 Server 的完整运行平均 CPU 通常低于 baseline；计算节点 CPU 略高。
- 10 核范围查询 batch helper 恢复了 bucket 数据的可见性，MVCC v35 的 `bucket_errors=0`，但吞吐比
  baseline 低 72.0%，尚未达到“性能损失不超过 30%”的目标。
- 10 核是当前较均衡的展示配置：Put、Get、混合负载分别提升 40.1%、5.1%、15.5%。

## 2. Baseline 与两侧算力部署方式

| 系统 | 计算侧职责 | 内存/存储侧职责 | 后台 CPU 的部署含义 | 本报告是否实测 |
|---|---|---|---|---|
| 分离式 RocksDB | 前台 Get/Put、LSM 元数据与通常的 flush/compaction 调度 | 提供远端内存或块存储 | 若后台工作仍在计算侧，扩容计算侧 CPU；若下推，则需单独配置存储侧 worker | 否，作为设计对照 |
| dLSM SST-only | 前台 Get/Put、memtable、版本管理 | 保存 SST，并执行近数据 compaction | node4 核数直接决定远端后台处理能力 | **是，本报告 baseline** |
| 本文优化版 dLSM | Put 只写 memtable；读路径合并 SST 与 bucket | flush 时把热 KV 写入 bucket/value zone，同时承担近数据 compaction | node4 同时承担 SST 后台任务和热区维护 | **是** |
| Disaggregated B-tree | 前台请求、上层节点缓存、遍历控制 | 保存树节点并处理远端节点访问 | 没有 LSM compaction，但可能需要节点分裂、合并和 RPC worker CPU | 否，作为结构对照 |
| CaaS（Compaction as a Service） | 前台 LSM 请求与 memtable/版本控制 | 独立 compaction 服务消费 SST 任务 | 应分别核算计算节点前台 CPU 与 compaction 服务 CPU，不能只报告总核数 | 否，作为部署对照 |

因此，本文的数值对比严格是“dLSM SST-only baseline 与 hot/cold flush 优化版”的公平对比；其他三类
baseline 用于说明部署与算力核算方法，不能用本次数据直接声称性能优劣。

## 3. 实验环境

### 3.1 硬件

| 项目 | node4（内存/Server） | node6（计算） |
|---|---|---|
| CPU | 2 × Intel Xeon Gold 5218R @ 2.10GHz | 2 × Intel Xeon Gold 5218R @ 2.10GHz |
| 核心 | 每路 20 核，2 线程/核，共 80 逻辑 CPU | 每路 20 核，2 线程/核，共 80 逻辑 CPU |
| NUMA | 2 个 NUMA 节点 | 2 个 NUMA 节点 |
| 内存 | 125GiB | 125GiB |
| RDMA | `mlx5_1` / `ens4f1`，NUMA 1 | `mlx5_1` / `ens3f1` |

Server 只绑定 NUMA 1 的物理核心，不使用 SMT sibling。核心扫描依次取
`1,3,5,...,31` 的前 N 个核心。没有强制内存 `membind`：node4 的单 NUMA 容量不足以容纳 Server 的
预分配与实验数据，强制绑定会触发 OOM。

### 3.2 数据集与参数

| 参数 | 设置 |
|---|---:|
| 数据分布 | RocksDB MixGraph/ZippyDB 风格，具有空间局部性 |
| 热区 | 3 个窄区间，约 90% 操作落入热区，约按 70/20/10 分配 |
| KV value | 固定 100,000B（约 100KB） |
| key | 20B 十进制 key，稀疏映射到完整物理 bucket key 空间 |
| load | 300,000 KV，约 30GB |
| 独立 Put | 160,000 次，约 16GB；约 50% update、50% insert |
| 独立 Get | 160,000 次；同时覆盖 load key 与 insert key |
| 混合负载 | 160,000 次；86% Get、14% Put、0% Seek |
| MemTable | 32MiB，已修正为按 `write_buffer_size` 正确切换 |
| SST/data block | 远端 SST 写块 8MiB |
| node6 前台线程 | 16 |
| node4 Server 核数 | 2、4、6、8、10、12、14、16 |

每轮顺序为：load → 等待后台完成 → Put → 等待 flush/compaction 完成 → Get → mixed。a、b、c
复用同一次 load，但每个 Server 核数和模式使用独立进程与数据实例。Put 前台只写 memtable；两版差异
只发生在后台 flush：baseline 写 SST，优化版将热 KV 写入 bucket/value zone。

## 4. a、b、c 整体结果

单位为 IOPS。优化版为 3 次均值 ± 样本标准差；baseline 为 1 次有效结果。

| Server 核数 | Baseline Put | 优化 Put | 提升 | Baseline Get | 优化 Get | 提升 | Baseline Mixed | 优化 Mixed | 提升 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 3,015 | 9,243 ± 1,035 | **+206.6%** | 88,229 | 105,372 ± 1,774 | **+19.4%** | 60,086 | 89,958 ± 33,172 | **+49.7%** |
| 4 | 7,542 | 16,311 ± 1,070 | **+116.3%** | 108,307 | 110,973 ± 3,383 | **+2.5%** | 105,722 | 112,600 ± 25,387 | **+6.5%** |
| 6 | 9,928 | 18,304 ± 187 | **+84.4%** | 107,478 | 110,213 ± 5,066 | **+2.5%** | 109,203 | 122,789 ± 4,716 | **+12.4%** |
| 8 | 11,993 | 18,928 ± 656 | **+57.8%** | 108,337 | 114,889 ± 590 | **+6.0%** | 114,514 | 123,781 ± 11,908 | **+8.1%** |
| 10 | 13,624 | 19,086 ± 525 | **+40.1%** | 108,129 | 113,678 ± 1,372 | **+5.1%** | 110,751 | 127,897 ± 7,363 | **+15.5%** |
| 12 | 14,548 | 19,158 ± 475 | **+31.7%** | 105,743 | 114,756 ± 986 | **+8.5%** | 111,109 | 125,408 ± 5,186 | **+12.9%** |
| 14 | 13,741 | 18,377 ± 106 | **+33.7%** | 102,812 | 113,905 ± 941 | **+10.8%** | 111,668 | 113,262 ± 25,308 | **+1.4%** |
| 16 | 15,104 | 20,091 ± 1,478 | **+33.0%** | 106,033 | 114,850 ± 402 | **+8.3%** | 110,788 | 129,284 ± 3,611 | **+16.7%** |

### 4.1 结果分析

- Put 是收益最明确的阶段。后台 CPU 紧张时，热 KV 不进入普通 SST 路径显著减轻远端后台压力；随着
  Server 核数增加，baseline 的瓶颈缓解，提升从 206.6% 收敛到约 33%。
- Get 提升较小但在全部核数上为正。降低 ValueZone 读锁范围并允许多个前台读线程并发后，热点读取不再
  被一把全局锁串行化。
- Mixed 的收益主要来自整体后台路径缩短，但波动大于纯 Put/Get。2、4、14 核的标准差较高，不能隐藏
  这些异常点；10、12、16 核的提升更适合作为展示结果。
- 10 核在三项指标之间最均衡，因此用于范围查询补充实验。

## 5. 10 核展示配置的延迟

| 指标 | Baseline | 优化版三次均值 | 变化 |
|---|---:|---:|---:|
| Put 平均延迟 | 1,152.353us | 817.555us | **-29.1%** |
| Put P95 | 2,166.613us | 2,358.210us | +8.8% |
| Put P99 | 5,026.774us | 5,999.949us | +19.4% |
| Get 平均延迟 | 142.980us | 135.474us | **-5.2%** |
| Get P95 | 170.079us | 150.122us | **-11.7%** |
| Get P99 | 182.911us | 161.463us | **-11.7%** |
| Mixed 平均延迟 | 138.886us | 120.649us | **-13.1%** |

优化版提高了 Put 平均吞吐和平均延迟，但 Put P95/P99 仍比 baseline 差，说明 flush、bucket 更新或共享
通信完成队列仍会造成少量长尾。Get 的平均和尾延迟均改善。

## 6. 两侧 CPU 与内存

完整进程周期的进程 CPU（100% 约等于占用一个逻辑核）：

| Server 核数 | node4 baseline | node4 优化 | node6 baseline | node6 优化 |
|---:|---:|---:|---:|---:|
| 2 | 144.3% | 149.2% | 166.4% | 192.5% |
| 4 | 229.4% | 213.6% | 212.4% | 229.4% |
| 6 | 279.8% | 254.0% | 220.9% | 244.6% |
| 8 | 324.5% | 291.5% | 229.3% | 248.9% |
| 10 | 347.7% | 314.8% | 233.7% | 250.3% |
| 12 | 368.3% | 330.6% | 234.1% | 255.1% |
| 14 | 382.9% | 329.8% | 237.6% | 254.2% |
| 16 | 381.2% | 340.2% | 239.5% | 254.9% |

4 核及以上，优化版 node4 平均少用约 0.16～0.53 个 CPU 核，同时完成更高的 Put/Mixed 吞吐；代价是
node6 多使用约 0.15～0.24 个核处理 bucket/value-zone 元数据与读合并。a、b、c 优化版 node4 峰值
RSS 约 88.4～89.6GiB，没有超过 node4 125GiB 物理内存。

## 7. d：范围查询

### 7.1 实现

普通 Iterator 只能看到 memtable/SST，无法看到已经从 SST flush 路径分离到 bucket/value zone 的热 KV。
新增的 scan helper 执行以下步骤：

1. node6 用普通 Iterator 扫描 SST 范围；
2. node6 向 node4 发送 `[start,end)` 的 `bucket_scan` 请求；
3. node4 查找范围内最新 bucket slot，在内存侧提取并排序 KV；
4. 以最多 8MiB 为一批，通过 RDMA Write 返回；超出时用 cursor 分页；
5. node6 合并 SST 与 bucket 结果，bucket 新版本覆盖 SST，删除记录移除旧值。

实现同时修复了 value-zone 记录跨 1GiB MR 和末尾 16B 对齐填充的解析问题。单条 bucket 解析错误会被
计数并允许跳过，但正式 v34 结果为 `bucket_errors=0`。

### 7.2 10 核结果

配置：160 个范围查询，每个查询覆盖 64 个逻辑 key；在 Put 后等待全部后台 flush/compaction 完成再开始。

| 指标 | Baseline | 优化版 v34 | 变化 |
|---|---:|---:|---:|
| 返回条目 | 12,785 | 12,784 | -1（-0.008%） |
| bucket 解析错误 | 不适用 | 0 | 正确 |
| 总耗时 | 244.296ms | 878.737ms | +259.7% |
| 查询吞吐 | 654.9 query/s | 182.1 query/s | **-72.2%** |
| 条目吞吐 | 52,334 entry/s | 14,548 entry/s | **-72.2%** |

返回条目数基本一致且 bucket 错误为零，说明 helper 恢复了热数据可见性。性能未达到目标，当前主要原因是
node4 每个范围请求都会线性遍历全部 `latest_bucket_sequence_by_key_`，并在扫描/提取期间持有
`bucket_update_mu_`；node6 的 helper RPC 还通过共享 `main_send_cq_mutex` 串行。后续应按 key 有序索引
或按 bucket/subrange 定位，只访问命中的 slot，并缩短锁范围。

本轮完整进程 CPU：baseline node4/node6 为 352.8%/188.5%，优化版为 346.5%/208.6%；峰值 RSS
分别约为 node4 88.52/89.33GiB、node6 3.59/3.84GiB。

## 8. 正确性与限制

- a、b、c 的所有 24 个优化版运行（8 个核数 × 3 次）均通过阶段标记和文件完整性检查。
- Get 在 Put 的后台 flush/compaction 完成后开始，避免读阶段被前一阶段后台工作污染。
- 正式 range v34：`bucket_errors=0`；两版返回条目仅差 1，属于并发 insert 顺序造成的极小结果集差异。
- baseline 每个核数只有 1 次，优化版有 3 次；baseline 没有误差条，跨版本比较的统计强度不对称。
- Mixed 在 2、4、14 核存在明显高方差，报告保留了标准差。
- Server 日志在指标完成后、进程关闭阶段会出现 RDMA completion 错误；计时阶段和结果标记均已完成。
- 本轮没有继续执行冷热分布切换和多节点扩展实验；多节点/多实例按要求暂不纳入结论。

## 9. 数据、代码与复现入口

- a、b、c 原始数据：`/home/k8s/liuyang/dlsm-zippy-cpu-sweep-20260907/results-v26-lock-scope`
- 三次重复汇总：`results-v26-lock-scope/analysis/repeated-summary.csv`
- 单轮明细：`results-v26-lock-scope/analysis/individual-runs.csv`
- a、b、c 表格：`results-v26-lock-scope/analysis/repeated-key-results.md`
- range baseline：`results-v26-lock-scope/range/post-helper/baseline-10c`
- range 正式优化版：`results-v26-lock-scope/range/post-helper-v34/hotcold-10c`
- 失败/诊断轮次保留在 `range/failed-attempts`、`range/post-helper-v30`、
  `range/post-helper-v31-debug`、`range/post-helper-v32-debug` 和 `range/post-helper-v33`。
- a、b、c runner：`reports/experiment-scripts/run_one.sh`
- CPU sweep runner：`reports/experiment-scripts/run_sweep.sh`
- range runner：`reports/experiment-scripts/run_range.sh`

最终结论应以本报告第 4、6、7、10 节及上述 v26/v34/v35 原始数据为准，旧报告中的早期单轮或错误 memtable
配置结果仅作为排障历史，不应继续作为论文主结果引用。

## 10. MVCC scan 并发改造（v35）

为避免只读 helper 在提取和复制 value 时长期持有全局更新锁，v35 增加了 bucket/value-zone MVCC：

- value-zone 新版本发布前，把旧 bucket head 写入记录的 `prev_offset`；
- bucket slot 最后发布新 head，value 记录发布后保持不可变；
- scan 只在共享锁内快照范围内的 key 与 head offset，随后在锁外按
  `sequence <= snapshot_sequence` 沿版本链选择可见版本；
- hot-to-cold 在共享锁下取得 slot 快照、锁外构建 SST，完成后仅清除仍指向该快照版本的 slot，
  不会删除迁移期间写入的新版本。

10 核、16 个 scan 线程、160 个查询、每次 64 个逻辑 key 的结果：

| 指标 | Baseline | batch helper v34 | MVCC helper v35 |
|---|---:|---:|---:|
| 返回条目 | 12,785 | 12,784 | 12,783 |
| bucket 错误 | 不适用 | 0 | 0 |
| 总耗时 | 244.296ms | 878.737ms | 871.629ms |
| 查询吞吐 | 654.9 query/s | 182.1 query/s | 183.6 query/s |
| 条目吞吐 | 52,334 entry/s | 14,548 entry/s | 14,666 entry/s |
| 相对 v34 | — | — | **+0.8%** |
| 相对 baseline | — | -72.2% | **-72.0%** |

完整运行平均进程 CPU：v35 node4 为 329.45%，node6 为 216.71%；峰值 RSS 分别为
88.53GiB 和 3.81GiB。

MVCC 解决了 bucket 元数据与不可变 value 版本之间的并发一致性，并把大 value 复制移出全局更新锁，
但对本轮吞吐改善很小。原因是 scan RPC 在 node6 仍由 `main_send_cq_mutex` 覆盖“发送到等待完整回复”
的全过程，16 个前台线程实际串行；node4 的 `Message_handler_pool_` 只有 2 个 worker；每个 RPC 仍线性
遍历全部 `latest_bucket_sequence_by_key_`。因此下一阶段要提高性能，应优先拆分每线程/每 QP 的完成队列
并建立按 key 或 subrange 有序的范围索引，而不是继续扩大 MVCC 锁优化范围。

v35 原始数据：
`/home/k8s/liuyang/dlsm-zippy-cpu-sweep-20260907/results-v26-lock-scope/range/post-helper-mvcc-v35/hotcold-10c`。
