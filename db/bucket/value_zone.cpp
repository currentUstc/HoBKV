#include "value_zone.h"
#include <cstring>
#include <cassert>
#include <unordered_map>
#include <unordered_set>
#include "util/coding.h"

namespace TimberSaw {

namespace {
constexpr size_t kValueZoneAlignment = VALUE_ZONE_OFFSET_UNIT;

size_t AlignUp(size_t value, size_t alignment) {
    return (value + alignment - 1) / alignment * alignment;
}
}  // namespace

Status AppendValueZoneEntry(const Slice& key, const Slice& value, uint64_t sequence_number,
                            bool is_deletion, std::string* destination, uint64_t* key_index_out) {
    if (destination == nullptr) {
        return Status::InvalidArgument("ValueZone: destination is null");
    }
    uint64_t key_index = 0;
    if (!key.TryGetIndex(&key_index)) {
        return Status::InvalidArgument("ValueZone: bucket keys must be unsigned decimal integers");
    }

    const size_t padded_key_size = AlignUp(key.size(), kValueZoneAlignment);
    const size_t padded_value_size = AlignUp(value.size(), kValueZoneAlignment);
    const size_t raw_total_size =
        6 * sizeof(uint64_t) + padded_key_size + padded_value_size + sizeof(uint64_t);
    const uint64_t total_size = static_cast<uint64_t>(AlignUp(raw_total_size, kValueZoneAlignment));

    PutFixed64(destination, total_size);
    PutFixed64(destination, key_index);
    PutFixed64(destination, sequence_number);
    PutFixed64(destination, is_deletion ? 1 : 0);
    PutFixed64(destination, key.size());
    PutFixed64(destination, value.size());
    destination->append(key.data(), key.size());
    destination->append(padded_key_size - key.size(), '\0');
    destination->append(value.data(), value.size());
    destination->append(padded_value_size - value.size(), '\0');
    PutFixed32(destination, 0);  // next
    PutFixed32(destination, 0);  // prev
    destination->append(total_size - raw_total_size, '\0');

    if (key_index_out != nullptr) {
        *key_index_out = key_index;
    }
    return Status::OK();
}

ValueZone::ValueZone(char* data, size_t max_size, std::shared_ptr<RDMA_Manager> rdma_mg)
    : max_size_(max_size > 16 ? max_size - 16 : 0),
      head(VALUE_ZONE_OFFSET_UNIT),
      tail(VALUE_ZONE_OFFSET_UNIT),
      rdma_mg_(rdma_mg) {
    assert(max_size_ > 0);
}

ValueZone::~ValueZone() {
    // 释放所有已分配的内存
    for (auto& mr : value_zone_mr_list_) {
        if (rdma_mg_) {
            rdma_mg_->Deallocate_Remote_RDMA_Slot(mr.addr, 0, ValueZoneChunk);
        }
    }
    value_zone_mr_list_.clear();
}

Status ValueZone::AllocSpace() {
    size_t current_total = value_zone_mr_list_.size() * VALUE_ZONE_ALLOC_SIZE;
    if (current_total >= VALUE_ZONE_MAX_SIZE) {
        return Status::IOError("ValueZone: Reached maximum allocated memory limit");
    }
    
    // 创建 ibv_mr 结构
    ibv_mr mr;
    
    // 如果需要，注册 RDMA 内存
    if (rdma_mg_) {
        // TODO: 根据实际 RDMA_Manager 接口注册内存
        // rdma_mg_->RegisterMemory(mr, VALUE_ZONE_ALLOC_SIZE, &mr);
        rdma_mg_->Allocate_Remote_RDMA_Slot(mr, 0, ValueZoneChunk);
    } else {
        return Status::IOError("ValueZone: RDMA Manager is not available for memory allocation");
    }
    
    // 添加到列表中
    value_zone_mr_list_.push_back(mr);
    
    return Status::OK();
}

Status ValueZone::Put(const Slice& key, const Slice& value, uint64_t sequence_number,
                      bool is_deletion, uint64_t& offset) {
    std::string entry;
    Status s = AppendValueZoneEntry(key, value, sequence_number, is_deletion, &entry, nullptr);
    if (!s.ok()) {
        return s;
    }
    return BatchedPut(entry, &offset);
}

Status ValueZone::BatchedPut(const std::string& batch, uint64_t* base_offset_units) {
    if (batch.empty()) {
        return Status::OK();
    }
    if (!rdma_mg_) {
        return Status::IOError("ValueZone: RDMA Manager is not available for batched put");
    }

    const size_t required_size = batch.size();
    uint64_t write_head = 0;
    std::vector<ibv_mr> remote_mrs;
    {
        // Reserve a disjoint append range and snapshot the MR descriptors.
        // Published bucket offsets are installed only after BatchedPut returns,
        // so readers cannot observe this range while its RDMA writes are in flight.
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (!HasSpace(required_size)) {
            return Status::IOError("ValueZone: not enough space for batched put");
        }

        while (NeedAllocSpace(required_size)) {
            Status s = AllocSpace();
            if (!s.ok()) {
                return s;
            }
        }

        write_head = head;
        head += required_size;
        remote_mrs = value_zone_mr_list_;
        if (base_offset_units != nullptr) {
            *base_offset_units = write_head / VALUE_ZONE_OFFSET_UNIT;
        }
    }

    size_t remaining = batch.size();
    const char* src = batch.data();
    ibv_mr local_mr = {};
    rdma_mg_->Allocate_Local_RDMA_Slot(local_mr, FlushBuffer);
    const size_t local_capacity = local_mr.length;

    while (remaining > 0) {
        const size_t mr_index = write_head / VALUE_ZONE_ALLOC_SIZE;
        const size_t offset_in_mr = write_head % VALUE_ZONE_ALLOC_SIZE;
        if (mr_index >= remote_mrs.size()) {
            rdma_mg_->Deallocate_Local_RDMA_Slot(local_mr.addr, FlushBuffer);
            return Status::IOError("ValueZone: write exceeds allocated memory");
        }

        const size_t bytes_in_mr = VALUE_ZONE_ALLOC_SIZE - offset_in_mr;
        const size_t chunk_len = std::min({remaining, bytes_in_mr, local_capacity});

        // TODO: 在 BucketTableBuilder 中直接构造 mr ，减少 memcpy 的数量。
        std::memcpy(local_mr.addr, src, chunk_len);

        ibv_mr remote_mr = remote_mrs[mr_index];
        remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + offset_in_mr);

        int ret = rdma_mg_->RDMA_Write(&remote_mr, &local_mr, chunk_len,
                                       "default", IBV_SEND_SIGNALED, 1, 0);
        if (ret != 0) {
            rdma_mg_->Deallocate_Local_RDMA_Slot(local_mr.addr, FlushBuffer);
            return Status::IOError("ValueZone: RDMA Write failed");
        }

        write_head += chunk_len;
        src += chunk_len;
        remaining -= chunk_len;
    }

    rdma_mg_->Deallocate_Local_RDMA_Slot(local_mr.addr, FlushBuffer);
    return Status::OK();
}

Status ValueZone::Get(uint64_t offset, Slice& value) {
    return GetWithSequence(offset, value, nullptr);
}

Status ValueZone::GetWithSequence(uint64_t offset, Slice& value, uint64_t* sequence_number,
                                  bool* is_deletion) {
    // offset uses VALUE_ZONE_OFFSET_UNIT-byte units and must be >= 1.
    if (offset < 1) {
        return Status::InvalidArgument("ValueZone: offset must be >= 1");
    }
    
    // 转换为字节偏移量
    uint64_t byte_offset = offset * VALUE_ZONE_OFFSET_UNIT;
    
    // 第一步：定位 MR 和计算相对偏移
    size_t mr_index = byte_offset / VALUE_ZONE_ALLOC_SIZE;
    size_t offset_in_mr = byte_offset % VALUE_ZONE_ALLOC_SIZE;

    auto snapshot_remote_mr = [this](size_t index, ibv_mr* result) {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (index >= value_zone_mr_list_.size()) {
            return false;
        }
        *result = value_zone_mr_list_[index];
        return true;
    };

    ibv_mr remote_mr;
    if (!snapshot_remote_mr(mr_index, &remote_mr)) {
        return Status::IOError("ValueZone: offset exceeds allocated memory");
    }

    // 第二步：第一次 RDMA Read - 读取 total_size (8B)
    remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + offset_in_mr);
    
    // 获取本地读取缓冲区
    ibv_mr* local_mr = rdma_mg_->Get_local_read_mr();
    
    // 读取 total_size (8B)
    int ret = rdma_mg_->RDMA_Read(&remote_mr, local_mr, 8,
                                  "read_local", IBV_SEND_SIGNALED, 1, 0);
    if (ret != 0) {
        return Status::IOError("ValueZone: First RDMA Read failed (total_size)");
    }
    
    // 解析 total_size
    uint64_t total_size;
    memcpy(&total_size, local_mr->addr, 8);
    
    if (total_size < 56 || total_size > 100 * 1024 * 1024) {  // 基本合法性检查
        return Status::Corruption("ValueZone: Invalid total_size");
    }
    
    // 第三步：第二次 RDMA Read - 根据 total_size 读取完整数据
    // total_size 包含了从 key_data 到 prev 的所有数据
    // 需要读取的大小 = total_size - 8 (不包括 total_size 自身)
    size_t data_to_read = total_size - 8;
    
    // 更新偏移量到 key_data 位置（跳过 total_size 的 8B）
    offset_in_mr += 8;
    if (offset_in_mr >= VALUE_ZONE_ALLOC_SIZE) {
        mr_index++;
        offset_in_mr = 0;
        if (!snapshot_remote_mr(mr_index, &remote_mr)) {
            return Status::IOError("ValueZone: data spans beyond allocated memory");
        }
    }
    
    // 分配缓冲区存储读取的数据
    char* data_buffer = new char[data_to_read];
    
    // 处理跨 MR 边界的情况
    size_t bytes_read = 0;
    while (bytes_read < data_to_read) {
        if (!snapshot_remote_mr(mr_index, &remote_mr)) {
            delete[] data_buffer;
            return Status::IOError("ValueZone: data spans beyond allocated memory");
        }
        
        // 计算本次读取大小
        size_t bytes_remaining_in_mr = VALUE_ZONE_ALLOC_SIZE - offset_in_mr;
        size_t bytes_to_read_now =
            std::min({bytes_remaining_in_mr, data_to_read - bytes_read, static_cast<size_t>(local_mr->length)});
        if (bytes_to_read_now == 0) {
            delete[] data_buffer;
            return Status::IOError("ValueZone: local RDMA read buffer has zero length");
        }
        
        // 执行 RDMA Read
        remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + offset_in_mr);
        
        ret = rdma_mg_->RDMA_Read(&remote_mr, local_mr, bytes_to_read_now,
                                  "read_local", IBV_SEND_SIGNALED, 1, 0);
        if (ret != 0) {
            delete[] data_buffer;
            return Status::IOError("ValueZone: Second RDMA Read failed");
        }
        
        // 复制到缓冲区
        memcpy(data_buffer + bytes_read, local_mr->addr, bytes_to_read_now);
        bytes_read += bytes_to_read_now;
        
        offset_in_mr += bytes_to_read_now;
        if (offset_in_mr == VALUE_ZONE_ALLOC_SIZE) {
            mr_index++;
            offset_in_mr = 0;
        }
    }
    
    // data_buffer starts immediately after total_size.
    if (sequence_number != nullptr) {
        *sequence_number = DecodeFixed64(data_buffer + 8);
    }
    if (is_deletion != nullptr) {
        *is_deletion = DecodeFixed64(data_buffer + 16) != 0;
    }
    const uint64_t key_size = DecodeFixed64(data_buffer + 24);
    const uint64_t value_size_raw = DecodeFixed64(data_buffer + 32);
    const size_t padded_key_size = AlignUp(static_cast<size_t>(key_size), kValueZoneAlignment);
    const size_t value_size = static_cast<size_t>(value_size_raw);
    const size_t parse_offset = 40 + padded_key_size;
    if (parse_offset + sizeof(uint64_t) > data_to_read ||
        value_size > data_to_read - parse_offset - sizeof(uint64_t)) {
        delete[] data_buffer;
        return Status::Corruption("ValueZone: invalid key/value sizes");
    }
    
    // 提取 value_data
    char* value_buffer = new char[value_size == 0 ? 1 : value_size];
    if (value_size > 0) {
        memcpy(value_buffer, data_buffer + parse_offset, value_size);
    }
    
    // 清理临时缓冲区
    delete[] data_buffer;
    
    // 设置返回值（忽略 next 和 prev）
    value = Slice(value_buffer, value_size);
    
    // 调用者负责释放 value 的内存
    return Status::OK();
}

}  // namespace TimberSaw
