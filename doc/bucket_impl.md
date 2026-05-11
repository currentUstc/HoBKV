# bucket impl




## hot2cold
将热区域对应 bucket 中的 KV 序列化为新的 SST ，放置在 最底 层

## cold2hot
冷区域切换为热区域
将该 bucket 分配至新的段。

方案分为两个部分：
- Sequence cut over: 记录冷热切换时的 sequence number
- - 新写入直接写 bucket。
- - 新读取先读 bucket ，若其 seq > 切换时的 seq ，无需读 SST 。

- 子范围完成位图：
- - 将一个 bucket 分为多个 shard 
- - 每个 shard 记录与其范围重叠的 SST ，存在这样的 SST 则状态记为“migrating”，反之则记为“bucket-only”
- - 读取的 KV 所在的范围为 “bucket-only” 时，也无需读取 SST 。

