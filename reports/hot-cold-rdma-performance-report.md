# dLSM 热冷分层 RDMA 性能测试报告

日期：2026-09-06

> 最新的 50GB、no-seek、update+insert 公平对比见第 11 节；该结果取代第 5、6 节的早期单轮结果作为当前结论。

## 1. 测试目标

验证开发分支的热数据 flush 到 bucket/value zone 方案能够正确运行，并在具有空间局部性的 ZippyDB/MixGraph key 分布下，与 SST-only baseline 做公平性能对比。

## 2. 环境与公平性约束

- 开发版本：`codex/hot-cold-rdma-fixes@5fd2f0f` 加当前工作区修复。
- baseline：相同代码与参数，仅将热迁移阈值设为 `UINT64_MAX`，数据只进入 SST。
- node4：内存节点，`REDACTED`，RDMA 设备 `mlx5_1` 位于 NUMA 1。
- node6：计算节点，`REDACTED`。
- node4 Server 固定到 NUMA 1 的 8 个物理核心：`1,3,5,7,9,11,13,15`，不使用 SMT sibling。
- node6 前台线程：16。
- 后台 flush 线程：4。
- memtable：32MB。
- value：固定 100KB。
- Bloom filter：10 bits。
- 初始化数据：约 20GB，200,000 条 KV。
- 初始化写不训练热点；正式 MixGraph 的 Get/Put 正常训练热点。
- 两版使用同一二进制源码、key 映射、线程数、memtable 和 CPU 绑定方式。

没有强制绑定 node4 内存 NUMA。尝试 `--membind=1` 时，Server 在约 53GB 预分配处耗尽 NUMA 1 的本地内存；因此只按要求绑定 CPU，允许内存跨 NUMA 分配。

## 3. ZippyDB/MixGraph 负载

key 生成逻辑基于 RocksDB 官方 MixGraph/ZippyDB social-graph 参数，并将逻辑 key 稀疏映射到完整的约 21.47 亿物理 key 空间。这样热点区间能够对应真实 bucket，而不是所有 key 都落入 bucket 0。

本轮 no-seek 负载：

- 86% Get。
- 14% Put。
- 0% Seek/Scan。
- 3 个窄热点区间，最终对应 shard 1、7、13。
- MixGraph Put 固定为 100KB，保证热点更新达到 32MB memtable 阈值并经过后台 flush。
- 每个版本执行 160,000 次 MixGraph 操作，约 22,167 次 Put，即约 2.22GB 更新数据，至少约 69 个 32MB memtable 容量。

去掉 Seek 的原因：原始 3% Seek 会在一次定位后连续扫描。此前每线程 154 次 Seek 扫描 78,992 条 100KB KV，平均每次约 513 条、约 51MB，范围扫描主导总耗时，但当前 bucket 优化只覆盖点查和 flush，不覆盖 Iterator 扫描。

## 4. 为正确测试增加的关键修复

1. 增加 `fillmixgraph`、`mixgraph`、`readmixgraph`，装载、负载和校验统一使用相同的稀疏物理 key 映射。
2. 初始化装载通过 `WriteOptions::track_hotness=false` 跳过热点训练，避免均匀装载提前占用 bucket；正常业务 Put 默认仍记录热度，写权重保持 2.0。
3. Cold-to-hot 后，bucket miss 回退 SST，以读取迁移前仍只存在于 SST 的旧数据。修复前优化版出现 `1,199/5,000` 未命中；修复后两版均为零未命中。
4. 消除同一次 Get 在 bucket miss 后重复执行第二次 bucket RDMA 查询。
5. MixGraph 分开统计 Get 和 Put 的总操作数、组件吞吐和平均调用延迟。
6. 大 value RDMA slot、读取缓存、CRC、MR 边界、空 compaction 输出等稳定性修复均包含在测试版本中。

## 5. 最终 no-seek 结果

| 指标 | Baseline | 优化版 | 变化 |
|---|---:|---:|---:|
| 初始化写吞吐 | 7,592 ops/s | 6,961 ops/s | -8.3% |
| MixGraph 总吞吐 | 123,560 ops/s | 131,711 ops/s | **+6.6%** |
| MixGraph Get 组件吞吐 | 106,438 ops/s | 113,453 ops/s | **+6.6%** |
| MixGraph Put 组件吞吐 | 17,118 ops/s | 18,246 ops/s | **+6.6%** |
| Get 平均延迟 | 96.386 us | 119.922 us | +24.4%（变慢） |
| Put 平均延迟 | 299.708 us | 83.241 us | **-72.2%** |
| 后置随机读校验吞吐 | 115,935 ops/s | 89,834 ops/s | -22.5% |
| 校验未命中 | 0 | 0 | 正确 |

Get/Put 组件吞吐以同一个 MixGraph 墙钟时间为分母，因此两者相加等于总吞吐。单操作平均延迟是分别围绕 Get/Put 调用测量的服务时间。整体提升主要来自 Put 延迟显著下降；Get 路径仍有额外 bucket RDMA 和锁竞争。

后置 `readmixgraph` 是全 key 空间正确性校验，不是 ZippyDB 热点性能指标。它本身记录读热度，运行期间会将更多均匀 shard 判热，因此其吞吐受 bucket miss/SST fallback 影响，主要用于确认 `not_found=0`。

## 6. 含 Seek 的 50GB 补充结果

配置为 50GB 初始化、83% Get、14% Put、3% Seek、固定 100KB value：

| 指标 | Baseline | 优化版 | 变化 |
|---|---:|---:|---:|
| 初始化写 | 7,363 ops/s | 8,728 ops/s | **+18.5%** |
| MixGraph 总吞吐 | 5,615 ops/s | 5,827 ops/s | **+3.8%** |
| 随机读校验 | 59,036 ops/s | 81,220 ops/s | **+37.6%** |
| 校验未命中 | 0 | 0 | 正确 |

含 Seek 时提升被 SST Iterator 的大范围扫描稀释，但优化版仍保持正收益。

## 7. node4 CPU 使用率

no-seek 20GB 测试期间，按计算节点日志的实际起止时间过滤采样：

| 指标 | Baseline | 优化版 |
|---|---:|---:|
| Server 进程平均 CPU | 47.50% | 48.51% |
| Server 进程峰值 CPU | 105% | 108% |
| 8 个绑定核心平均利用率 | 7.34% | 6.92% |
| 单核心峰值 | 100% | 100% |

负载明显集中到一个核心：baseline 最忙核心平均约 41.7%，优化版最忙核心平均约 42.6%，其余多数核心长期低于 10%。因此当前并非 8 核算力耗尽，而是通信/同步路径串行化导致核心利用不均。

## 8. 剩余瓶颈

1. `BucketTableHandler::GetOffsetForRegionKey` 使用全局 mutex，并在锁内执行同步 bucket-table RDMA read。
2. `ValueZone::GetWithSequence` 使用 ValueZone 全局 mutex，锁覆盖完整读取流程。
3. bucket 命中通常需要 bucket offset、value header、完整 value 三次同步 RDMA；SST data block 路径通常更少。
4. 每次 value-zone Get 为 100KB value 动态分配缓存、复制到字符串后释放。
5. `RDMA_Manager::Remote_Query_Pair_Connection` 使用 `main_send_cq_mutex`，多个读线程在 QP 路径排队。
6. `MemTable::Add` 的粗粒度 `table_write_mutex_` 串行化前台并发 Put。
7. 初始化写在两次 no-seek 单次样本间存在约 8% 波动，需要多轮重复测试给出均值和置信区间。

## 9. 结论

在 node4 绑定 8 个 RDMA-local NUMA 物理核心、20GB 初始化、32MB memtable、固定 100KB value、3 个空间热点、86/14 Get/Put 的 no-seek ZippyDB key 分布下：

- baseline 与优化版均完整结束且数据校验通过。
- 热点准确落入 shard 1、7、13 的 bucket。
- 优化版 MixGraph 总吞吐提高 6.6%。
- 优化版 Put 平均延迟降低 72.2%，是整体提升的主要来源。
- Get 延迟仍慢 24.4%，后续优化重点应是拆分 bucket/value-zone 全局锁、减少同步 RDMA 次数和消除 100KB 临时分配/复制。

## 10. 日志位置

远端目录：`/tmp/dlsm-hotcold-20260906-1349`

- baseline compute：`compute-baseline-zippy100k-noseek-20g-8c.log`
- optimized compute：`compute-hotcold-zippy100k-noseek-20g-8c.log`
- baseline server：`server-baseline-zippy100k-noseek-20g-8c.log`
- optimized server：`server-hotcold-zippy100k-noseek-20g-8c.log`
- CPU：`cpu-{baseline,hotcold}-zippy100k-noseek-20g-8c-{pidstat,mpstat}.log`

## 11. 最新 50GB no-seek update+insert 对比

### 11.1 公平性与负载

- baseline 和优化版使用完全相同的 `db_bench` 与 Server 二进制；baseline 仅通过
  `--enable_hot_cold=0` 将热度阈值等效设为无限大，关闭 bucket 分类与迁移。
- node4 Server 固定到 RDMA 所在 NUMA 1 的物理核心 `1,3,5,7,9,11,13,15`；node6 使用 16 个前台线程。
- load：每线程 31,250 个 KV，共 500,000 个 100KB KV，即 50.0GB 十进制数据（约 46.57GiB）。
- memtable：32MB；load 完成后等待后台 flush，确保初始数据全部经过 SST flush。
- MixGraph：每线程 50,000 次，共 800,000 次；86% Get、14% Put、0% Seek。
- Put 固定 100KB，共 111,783 次、约 11.18GB 写入流，足以触发约 349 个 32MB memtable 容量。
- Put 分为 55,752 次 update 和 56,031 次真正 insert；insert 使用 load 未占用的新物理 key，并保持相同空间分布。
- 90% 的 Get/Put 落入 3 个窄热点区间，其余 10% 分散到另外 27 个区间。三个热点内部为约 70/20/10。
- 后置正确性校验每线程 5,000 次，共 80,000 次，只校验 load key 空间，避免长校验再次改变主负载状态。

两版由确定性随机种子产生完全相同的操作：688,217 Get、111,783 Put；Get 热点比例均为 89.96%，Put 热点比例均为 90.13%。三个热点操作数也逐项一致。

### 11.2 性能结果

| 指标 | Baseline | 优化版 | 变化 |
|---|---:|---:|---:|
| 50GB load | 8,572 ops/s | 8,773 ops/s | **+2.3%** |
| MixGraph 总吞吐 | 63,306 ops/s | 119,517 ops/s | **+88.8%** |
| Get 组件吞吐 | 54,460 ops/s | 102,815 ops/s | **+88.8%** |
| Put 组件吞吐 | 8,846 ops/s | 16,700 ops/s | **+88.8%** |
| Get 平均延迟 | 67.832 us | 86.324 us | +27.3% |
| Get P50 / P95 / P99 | 26.412 / 157.232 / 197.731 us | 80.179 / 215.554 / 353.251 us | 变慢 |
| Put 平均延迟 | 1,352.473 us | 410.737 us | **-69.6%** |
| Put P50 | 872.093 us | 366.853 us | **-57.9%** |
| Put P95 | 4,136.764 us | 920.722 us | **-77.7%** |
| Put P99 | 7,171.933 us | 1,345.235 us | **-81.2%** |
| 后置校验 | 62,820 ops/s | 71,246 ops/s | **+13.4%** |
| 校验未命中 | 0 | 0 | 正确 |

Get/Put 组件吞吐均以同一个 MixGraph 墙钟时间为分母；单操作延迟则分别包围实际 `Get`/`Put` 调用。优化版 Get 仍受 bucket 多次同步 RDMA 和全局锁影响，但 Put 的平均与尾延迟大幅下降，足以使总吞吐提升 88.8%。

### 11.3 bucket 与 flush 证据

优化版运行时只分配了 3 个 512MB bucket table，并记录了恰好 3 次 cold-to-hot：

```text
shard=13, key_range=[1744830464,1879048192)
shard=7,  key_range=[939524096,1073741824)
shard=1,  key_range=[134217728,268435456)
```

这与 workload 的三个空间热点一致。热区确认后，`FlushJob::BuildTable` 对 `IsHotKey()` 为真的 KV 调用 `BucketTableBuilder::Add/Finish`，由 `ValueZone::BatchedPut` 写 value zone，再通过 `flush_helper` 更新 bucket offset；baseline 没有任何 cold-to-hot 或 bucket 分配记录。

### 11.4 node4 CPU

| 指标（完整进程周期） | Baseline | 优化版 |
|---|---:|---:|
| Server 平均 CPU | 64.42% | 62.09% |
| Server 峰值 CPU | 200% | 201% |
| 8 个绑定核心平均利用率 | 9.06% | 8.85% |

优化版以略低的平均 Server CPU 完成更高吞吐。负载仍不均衡，最忙绑定核心平均约 36%–38%，说明后续仍应优先拆分通信和 bucket/value-zone 全局锁。

### 11.5 最新日志

目录：`/tmp/dlsm-hotcold-20260906-1349`

- `compute-baseline-zippy50g-v4-8c.log`
- `compute-hotcold-zippy50g-v4-8c.log`
- `server-baseline-zippy50g-v4-8c.log`
- `server-hotcold-zippy50g-v4-8c.log`
- `cpu-{baseline,hotcold}-zippy50g-v4-8c-{pidstat,mpstat}.log`

## 12. 独立 Put / Get 复测（30GB load + 16GB Put）

### 12.1 为什么重测

第 11 节混合负载中的 Get/Put“组件吞吐”共享同一个 MixGraph 墙钟时间，因此两者都显示
88.8% 提升，只能表示总负载完成时间缩短，不能解释为独立 Get 和 Put 均提升 88.8%。本轮将
Put 和 Get 拆成两个串行阶段，并在 Put 前台统计结束后等待全部 memtable flush 完成，再启动 Get。

最初尝试 50GB load + 16GB Put 和 40GB load + 16GB Put 时，node4 Server 分别在后台 flush
期间被 OOM killer 终止。40GB 轮次的内核记录显示 Server 峰值 RSS 约 110.9GB，而 node4 另有
约 16GB 常驻内存。最终保持 32MB memtable，将 load 降为 30GB。

### 12.2 配置

- load：300,000 个 100KB KV，共 30GB。
- Put：160,000 次，共 16GB 写入流；79,935 update，80,065 insert。
- Get：160,000 次；约一半访问 load key，另一半访问本轮 insert key。
- 90% 操作位于三个空间热点；两版使用完全相同的确定性操作序列。
- node4 Server 绑定 NUMA 1 的 8 个物理核心 `1,3,5,7,9,11,13,15`。
- baseline 和优化版使用同一二进制，只切换 `--enable_hot_cold=0/1`。

### 12.3 独立性能结果

| 指标 | Baseline | 优化版 | 变化 |
|---|---:|---:|---:|
| 30GB load IOPS | 6,826 | 7,160 | +4.9% |
| Put IOPS | 8,079 | 7,478 | **-7.4%** |
| Put 平均延迟 | 1,791.098 us | 1,938.456 us | +8.2% |
| Put P50 | 359.918 us | 1,008.313 us | +180.2% |
| Put P95 | 6,405.816 us | 5,552.785 us | -13.3% |
| Put P99 | 19,636.171 us | 20,731.125 us | +5.6% |
| Get IOPS | 107,724 | 17,042 | **-84.2%** |
| Get 平均延迟 | 143.049 us | 933.215 us | +552.4% |
| Get P50 | 142.338 us | 1,008.459 us | +608.5% |
| Get P95 | 168.736 us | 1,048.628 us | +521.5% |
| Get P99 | 199.420 us | 1,189.330 us | +496.3% |
| Get 未命中 | 0 / 160,000 | **4,176 / 160,000** | 优化版丢失 2.61% |
| 后置 load-key 校验未命中 | 0 / 80,000 | 0 / 80,000 | 通过 |

优化版记录的 cold-to-hot 恰好发生在热点 shard 1、7、13。后置 load-key 校验通过，但独立 Get
对 Put 阶段 insert key 出现 4,176 次未命中，说明正确性问题集中在 Put flush 后的
bucket/value-zone 路径。在修复该问题前，不能将优化版视为可正确运行，也不能用本轮 Get 数字
主张读性能提升。

### 12.4 node4 CPU

| 完整进程周期 | Baseline | 优化版 |
|---|---:|---:|
| Server 平均 CPU | 52.40% | 43.58% |
| 非空闲采样平均 CPU | 63.99% | 55.53% |
| Server 峰值 CPU | 105% | 101% |

### 12.5 日志

目录：`/tmp/dlsm-hotcold-20260906-1349`

- `compute-baseline-zippy30g-put16g-v14-8c.log`
- `compute-hotcold-zippy30g-put16g-v14-8c.log`
- `server-baseline-zippy30g-put16g-v14-8c.log`
- `server-hotcold-zippy30g-put16g-v14-8c.log`
- `cpu-{baseline,hotcold}-zippy30g-put16g-v14-8c-{pidstat,mpstat}.log`

### 12.6 优化版重复运行

使用完全相同参数再次运行优化版，结果如下：

| 指标 | 第一次 | 第二次 |
|---|---:|---:|
| Load IOPS | 7,160 | 7,499 |
| Put IOPS | 7,478 | 7,738 |
| Put 平均延迟 | 1,938.456 us | 1,827.709 us |
| Get IOPS | 17,042 | 16,848 |
| Get 平均延迟 | 933.215 us | 943.876 us |
| Get 未命中 | 4,176（2.61%） | 3,872（2.42%） |
| 后置 load-key 校验未命中 | 0 | 0 |
| Server 平均 CPU | 43.58% | 43.41% |

第二次仍只迁移 shard 1、7、13。Get 吞吐、延迟和未命中比例与第一次高度接近，说明异常可稳定
复现，并非单次测量噪声。第二次日志为 `compute-hotcold-zippy30g-put16g-v15-8c.log`、
`server-hotcold-zippy30g-put16g-v15-8c.log` 和对应的 `cpu-hotcold-...-v15-8c-*` 文件。

### 12.7 Put IOPS 与实际 memtable 切换条件更正

正常写路径并未使用 `options.write_buffer_size` 判断 memtable 是否已满，而是使用固定的
`MEMTABLE_SEQ_SIZE=153846` 条记录作为序列号边界。100KB value 下，一个满 memtable 实际约为
15.4GB。因此本轮16GB Put只产生一个完整 memtable flush和一个尾部 flush，不是此前估算的约
500次32MB flush。`--write_buffer_size=32MB` 仅在日志恢复路径中按内存占用判断。

加入临时只读计数后，baseline 与优化版在 load 和 Put 阶段均为：

```text
slowdowns=0 stalls=0 stall_wait_us=0
```

所以 Put 差异不是 L0/immutable write stall。重复配对结果也不支持稳定退化：

| 配对 | Baseline Put IOPS | 优化版 Put IOPS | 变化 |
|---|---:|---:|---:|
| 初次完整运行 | 8,079 | 7,478 | -7.4% |
| 写压计数运行 | 7,378 | 7,698 | +4.3% |

不同轮次之间约有5%–10%波动，当前只能认为两版纯 Put 吞吐接近，不能声称优化版 Put 稳定下降。
优化版的中位延迟较高，但写压计数轮的 P95/P99 更低，平均延迟几乎相同（baseline 1,860.5us，
优化版1,869.8us）。

此前混合负载没有发现新增 key 丢失，是因为 mixed 模式中的 Get 只生成原始 load key，且后置
`readmixgraph` 同样只校验 load key。来源分类复测确认：80,167次 load-key Get 全部命中；
79,833次 insert-key Get 中有3,850次未命中。该问题完全来自 Put 阶段的 insert key 路径。

## 13. 32MB memtable 与并发热读修复

修正 memtable 按 `write_buffer_size` 切换后，100KB value、32MB write buffer 对应每个 memtable
约 334 条记录。使用 30GB load、16GB Put、160,000 次独立 Get、16 个前台线程重新测试。

修复前，热读依次经过 bucket slot、value-zone header 和 value 三次同步 RDMA Read；
`BucketTableHandler` 与 `ValueZone` 又分别用一把全局互斥锁覆盖 RDMA 操作，导致 16 个线程串行。
修复将只读元数据访问改为共享锁，并将 bucket slot RDMA Read 移出元数据锁；value-zone 扩容和
写入仍使用独占锁，以保护 MR 列表和写入位置。

| 指标 | 修复前（v21） | 并发读（v22） | 变化 |
|---|---:|---:|---:|
| Load IOPS | 11,276 | 11,402 | +1.1% |
| Put IOPS | 19,036 | 18,851 | -1.0% |
| Get IOPS | 17,397 | **111,495** | **+540.9%** |
| Get 平均延迟 | 914.189 us | **138.227 us** | **-84.9%** |
| Get P50 | 988.844 us | **130.026 us** | **-86.9%** |
| Get P95 | 1,057.047 us | **150.364 us** | **-85.8%** |
| Get P99 | 1,815.012 us | **162.765 us** | **-91.0%** |

并发读版本的 Get 达到 111,495 IOPS，略高于同配置 baseline 的 105,961 IOPS；Put 吞吐变化在
正常轮次波动范围内。本轮按要求未执行最终 `readmixgraph` check。

日志：

- `compute-hotcold-zippy30g-put16g-memtable32m-v21-8c.log`
- `compute-hotcold-concurrent-read-zippy30g-put16g-v22-8c.log`
- `server-hotcold-concurrent-read-zippy30g-put16g-v22-8c.log`
- `cpu-hotcold-concurrent-read-zippy30g-put16g-v22-8c-node{4,6}.log`

## 14. 并发读修复后的完整配对测试

baseline 和优化版使用同一份 `v22` 二进制与相同确定性操作序列，仅切换
`--enable_hot_cold=0/1`。配置保持为 30GB load、16GB Put、160,000 次 Get、100KB value、
32MB memtable、node6 16 个前台线程；node4 Server 固定到 NUMA 1 的 8 个物理核心。
按要求未执行最终 `readmixgraph` check。

| 指标 | Baseline | 优化版 | 变化 |
|---|---:|---:|---:|
| Load IOPS | 10,572 | 11,118 | +5.2% |
| Put IOPS | 11,486 | **18,939** | **+64.9%** |
| Put 平均延迟 | 1,344.876 us | **824.763 us** | **-38.7%** |
| Put P50 | 554.037 us | 550.585 us | -0.6% |
| Put P95 | 2,380.669 us | 2,459.647 us | +3.3% |
| Put P99 | 5,209.621 us | 5,837.324 us | +12.1% |
| Get IOPS | 103,452 | **113,494** | **+9.7%** |
| Get 平均延迟 | 149.777 us | **135.578 us** | **-9.5%** |
| Get P50 | 154.667 us | **129.695 us** | **-16.1%** |
| Get P95 | 177.113 us | **149.486 us** | **-15.6%** |
| Get P99 | 189.405 us | **160.650 us** | **-15.2%** |

node4 Server 完整有效采样平均 CPU：baseline 325.27%，优化版 299.55%；峰值分别为 794% 和
799%。这说明修正 32MB memtable 后后台 flush/compaction 能实际使用多个绑定核心；优化版在更短
时间内完成负载，平均 Server CPU 仍低约 0.26 个核。

日志：

- `compute-{baseline,hotcold}-paired-v23-8c.log`
- `server-{baseline,hotcold}-paired-v23-8c.log`
- `cpu-{baseline,hotcold}-paired-v23-node{4,6}.log`
