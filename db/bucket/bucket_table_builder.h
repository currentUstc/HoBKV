

/*  BucketTableBuilder compute side:
        1. 使用 add() 接收 UserKey 和 Value，将其逐个序列化为 #value_zone.h 中定义的条目格式，并储存 <key, offset>
        2. <key, offset> 表示一个用户键和它在本个 builder 中的偏移量。
        3. 调用 finish 时向 value zone 中写入所有条目，并记录 value_zone 中的 current offset，与每个 key 对应的 offset 相加。
        4. 将所有的 <key, offset> 序列化，发起一个 flush_helper 请求，在内存侧将对应的 offset 写入对应的 bucket table 中。
*/

#ifndef BUCKET_TABLE_BUILDER_H
#define BUCKET_TABLE_BUILDER_H

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "TimberSaw/slice.h"
#include "TimberSaw/status.h"
#include "value_zone.h"

namespace TimberSaw {

class RDMA_Manager;

class BucketTableBuilder {
public:
    struct BucketUpdate {
        uint64_t key_index;
        uint64_t offset_units;
        uint64_t sequence_number;
        bool is_deletion;
    };

    explicit BucketTableBuilder(const std::shared_ptr<ValueZone>& value_zone);
    BucketTableBuilder(const std::shared_ptr<ValueZone>& value_zone, const std::shared_ptr<RDMA_Manager>& rdma_mg,
                       uint8_t target_node_id);
    ~BucketTableBuilder() {}

    void Add(const Slice& key, const Slice& value, uint64_t sequence_number = 0,
             bool is_deletion = false);
    
    Status Finish();

    const std::string& value_zone_buffer() const { return value_zone_buffer_; }
    const std::vector<BucketUpdate>& bucket_updates() const { return bucket_updates_; }
    uint64_t value_zone_base_offset_units() const { return value_zone_base_offset_units_; }

private:
    static constexpr size_t kAlignBytes = VALUE_ZONE_OFFSET_UNIT;
    static constexpr size_t kUpdatePairBytes = sizeof(uint64_t) * 4;

    struct PendingEntry {
        uint64_t key_index;
        uint64_t local_offset_bytes;
        uint64_t sequence_number;
        bool is_deletion;
    };

    struct UpdateBatch {
        std::string buffer;
        size_t kv_count = 0;
    };

    std::shared_ptr<ValueZone> value_zone_;
    std::shared_ptr<RDMA_Manager> rdma_mg_;
    uint8_t target_node_id_ = 0;
    std::vector<PendingEntry> pending_entries_;
    std::vector<BucketUpdate> bucket_updates_;
    std::vector<UpdateBatch> update_batches_;
    std::string value_zone_buffer_;
    uint64_t local_offset_bytes_ = 0;
    uint64_t value_zone_base_offset_units_ = 0;
    bool finished_ = false;
    Status status_;

};

}

#endif  // BUCKET_TABLE_BUILDER_H
