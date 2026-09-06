#ifndef VALUE_ZONE_H
#define VALUE_ZONE_H

#include <include/TimberSaw/slice.h>
#include <include/TimberSaw/status.h>
#include <include/TimberSaw/options.h>
#include <util/rdma.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <cstdint>

namespace TimberSaw {

// Serialized entry format (all scalar fields are fixed64):
// [total_size, key_index, sequence, type, key_size, value_size,
//  padded key, padded value, next+prev]
Status AppendValueZoneEntry(const Slice& key, const Slice& value, uint64_t sequence_number,
                            bool is_deletion, std::string* destination, uint64_t* key_index);

/*  
    Value Zone for storing values.
        Values are stored in a contiguous memory region.
    Entry format (每个字段8B对齐):
        [total_size, key_index, sequence_number, value_type, key_size, value_size,
         key_data, value_data, next(4B) + prev(4B)]
        原始 key 会被保留，以便热转冷时重建完全相同的 SST key。
        next and prev are offsets to entries with the same key (forming a doubly linked list).
        offset of 0 means null (end of list).
*/
class ValueZone {
public:
    // 构造函数接收外部分配的内存指针
    ValueZone(char* data, size_t max_size, std::shared_ptr<RDMA_Manager> rdma_mg);
    ~ValueZone();

    // Put: 追加写入一个新条目，返回该条目的 offset
    // 如果已存在相同 key 的条目，将新条目链接到链表中
    Status Put(const Slice& key, const Slice& value, uint64_t sequence_number,
               bool is_deletion, uint64_t& offset);

    // 在 BucketTableBuilder 中调用 BatchedPut 来写入全部条目，同一时间仅能有一个写入操作。
    Status BatchedPut(const std::string& batch, uint64_t* base_offset_units);
    
    // Get: 根据 offset 读取一个条目，返回该条目以及通过链表关联的所有条目
    // 返回格式: vector of (sequence_number, value)
    Status Get(uint64_t offset, Slice &value);
    Status GetWithSequence(uint64_t offset, Slice& value, uint64_t* sequence_number,
                           bool* is_deletion = nullptr);
    
    Status AllocSpace();

    uint64_t GetCurrentOffset() const { return head; }
    
    // 获取当前已使用的空间大小 (head - tail)
    uint64_t GetUsedSpace() const { return head - tail; }

    // 检查是否有足够的空间（考虑 tail 到 head 之间的已用空间）
    bool HasSpace(size_t required_size) const { 
        // 已使用空间 = head - tail
        // 可用空间需要考虑是否达到最大限制
        // if (tail < head) {
        //     return (head - tail + required_size) <= max_size_;
        // } else if (tail > head) {
        //     return (max_size_ - (tail - head) + required_size) <= max_size_;
        // } else {
        //     return required_size <= max_size_;
        // }

        //当前不考虑清理
        return head + required_size <= max_size_;
    }
    
    // 检查是否需要分配新的内存块
    bool NeedAllocSpace(size_t required_size) const {
        size_t total_allocated = value_zone_mr_list_.size() * VALUE_ZONE_ALLOC_SIZE;
        return head + required_size > total_allocated;
    }

private:

    // char* data_;                                 // 数据区指针（外部分配）
    static const size_t key_size_ = 8;              // key size 固定为 8B
    size_t max_size_;
    uint64_t head;
    uint64_t tail;
    std::shared_ptr<RDMA_Manager> rdma_mg_;   // RDMA 管理器
    std::vector<ibv_mr> value_zone_mr_list_;                    // RDMA 内存注册
    mutable std::mutex mutex_;
};

}

#endif  // VALUE_ZONE_H
