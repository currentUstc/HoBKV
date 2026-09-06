# 冷热区域调整机制说明

## 概述

实现了一个基于 LRU 策略的冷热区域自动调整机制，可以根据访问热度动态调整 key range 的冷热状态。

## 核心组件

### 1. 数据结构

#### HotnessConfig（热度配置）
```cpp
struct HotnessConfig {
    uint64_t check_interval_us = 10000000;  // 检查间隔：10秒
    uint64_t hot_threshold = 1000;           // 热度阈值：访问次数
    double decay_factor = 0.9;               // 衰减因子
} hotness_config_;
```

#### LRU 管理
```cpp
std::list<size_t> hot_region_lru_;  // LRU 列表，存储热区域的 shard 索引
std::mutex hot_stats_mutex_;         // 保护热度统计和 LRU 列表
```

### 2. 核心函数

#### RecordKeyAccess()
- 每次 Get/Put/Delete 时调用
- 更新对应 shard 的访问统计
- 定期触发热度调整

#### UpdateHotnessScore()
- 计算每个 shard 的热度评分
- 应用衰减因子到访问计数

#### AdjustHotColdRegions()
- 主调整逻辑，执行以下步骤：
  1. 更新所有 shard 的热度评分
  2. 找出超过热度阈值的 shard
  3. 按热度降序排序
  4. 选取最热的最多 `max_bucket_tables` 个区域
  5. 使用 LRU 策略替换冷热区域
  6. 对每个状态变化调用 `Hot2Cold()` 或 `Cold2Hot()`

#### MaybeScheduleHotnessAdjustment()
- 检查是否需要调度热度调整
- 使用 CAS 避免重复调度
- 间隔时间由 `check_interval_us` 控制

## 工作流程

```
访问 Key (Get/Put/Delete)
    ↓
RecordKeyAccess()
    ├─ 更新 shard 的访问统计
    └─ MaybeScheduleHotnessAdjustment()
        ↓
    [每 10 秒一次]
        ↓
BackgroundHotnessAdjustment()
    ↓
AdjustHotColdRegions()
    ├─ UpdateHotnessScore()
    │   └─ 计算热度 & 应用衰减
    ├─ 识别热门 shard
    ├─ LRU 替换逻辑
    │   ├─ Hot → Cold: 调用 Hot2Cold(shard_idx)
    │   └─ Cold → Hot: 调用 Cold2Hot(shard_idx)
    └─ 更新 LRU 列表
```

## LRU 替换策略

1. **最多热区域数限制**：`max_bucket_tables`（默认 16）
2. **选择策略**：按热度评分降序选择
3. **替换策略**：
   - 当新热区域数超过限制时
   - 从 LRU 列表尾部移除最久未访问的热区域
   - 将该区域标记为冷，调用 `Hot2Cold()`
4. **LRU 更新**：
   - 新晋热区域插入列表头部
   - 仍然活跃的热区域移到头部
   - 变冷的区域从列表移除

## 热度计算

```cpp
hotness_score = access_count * decay_factor
```

每次调整后，访问计数会衰减：
```cpp
new_access_count = old_access_count * decay_factor
```

这确保了只有持续访问的区域才能保持热状态。

## 用户回调

系统会在冷热状态变化时调用以下回调：

```cpp
Status DBImpl::Hot2Cold(size_t shard_idx);  // 热 → 冷
Status DBImpl::Cold2Hot(size_t shard_idx);  // 冷 → 热
```

参数 `shard_idx` 是发生变化的 key range shard 索引。

## 配置调优

可以通过修改 `hotness_config_` 调整行为：

- `check_interval_us`：调整检查频率（默认 10 秒）
- `hot_threshold`：访问次数阈值（默认 1000）
- `decay_factor`：热度衰减速度（默认 0.9，范围 0-1）

## 线程安全

- 访问统计使用原子操作
- LRU 列表由 `hot_stats_mutex_` 保护
- 调度使用 CAS 避免竞争

## 监控输出

每次冷热切换会打印日志：
```
Shard 5: Cold -> Hot (access: 1500, score: 1350.00)
Shard 12: Hot -> Cold (access: 800, score: 720.00)
Shard 3: Hot -> Cold (LRU eviction, access: 900, score: 810.00)
```

## 示例场景

假设 `max_bucket_tables = 3`，`hot_threshold = 1000`：

1. **初始状态**：所有区域都是冷的
2. **访问累积**：Shard 0, 1, 5 访问量超过 1000
3. **第一次调整**：
   - Shard 0, 1, 5 变热
   - LRU: [5, 1, 0]
4. **继续访问**：Shard 2 访问量超过 1000，Shard 0 不再访问
5. **第二次调整**：
   - Shard 0 热度衰减，低于阈值
   - Shard 2 新晋热区域
   - Shard 0 变冷（调用 Hot2Cold(0)）
   - Shard 2 变热（调用 Cold2Hot(2)）
   - LRU: [2, 5, 1]
