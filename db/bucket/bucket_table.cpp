#include "bucket_table.h"
#include "TimberSaw/options.h"
#include <cstring>
#include <cassert>
#include <cstdio>

namespace TimberSaw {

BucketTable::BucketTable(Slice min_key, Slice max_key, std::shared_ptr<RDMA_Manager> rdma_mg) 
    : rdma_mg_(rdma_mg), owns_remote_memory_(true) {
    const bool valid_range = min_key.TryGetIndex(&min_key_index_) &&
                             max_key.TryGetIndex(&max_key_index_) &&
                             min_key_index_ < max_key_index_;
    assert(valid_range);
    // Allocate the bucket data from RDMA managed memory pool
    // Maybe we need to allocate memory outside BucketTable class, and passing the rdma_mg pointer here.
    // rdma_mg_->Allocate_Local_RDMA_Slot(bucket_mr_, BucketChunk);
    rdma_mg_->Allocate_Remote_RDMA_Slot(bucket_mr_, 0, BucketChunk);
    // bucket_data_ = static_cast<char*>(bucket_mr_.addr);
    bucket_data_ = static_cast<char*>(bucket_mr_.addr);
    printf("BucketTable allocated: addr=%p, size=%llu MB\n", bucket_data_, BUCKET_SIZE/(1024*1024));
}

BucketTable::BucketTable(Slice min_key, Slice max_key, const ibv_mr& bucket_mr, std::shared_ptr<RDMA_Manager> rdma_mg)
    : rdma_mg_(rdma_mg), bucket_mr_(bucket_mr), owns_remote_memory_(false) {
    const bool valid_range = min_key.TryGetIndex(&min_key_index_) &&
                             max_key.TryGetIndex(&max_key_index_) &&
                             min_key_index_ < max_key_index_;
    assert(valid_range);
    bucket_data_ = static_cast<char*>(bucket_mr_.addr);
}

BucketTable::~BucketTable() {
    // Deallocate the bucket data
    if (owns_remote_memory_ && bucket_data_ != nullptr && rdma_mg_ != nullptr) {
        rdma_mg_->Deallocate_Remote_RDMA_Slot(bucket_data_, 0, BucketChunk);
        bucket_data_ = nullptr;
    }
}

// Status BucketTable::update(const Slice& key, const unsigned offset) {
//     // Check if key is within the valid range [min_key_, max_key_]
//     if (key.compare(min_key_) < 0 || key.compare(max_key_) > 0) {
//         return Status::InvalidArgument("Key out of bucket range");
//     }
    
//     // Calculate the slot index: key - min_key
//     unsigned index = key.Index() - min_key_.Index();
    
//     // Check if index is within the bucket size
//     if (index >= SLOT_NUM_PER_BUCKET) {
//         return Status::InvalidArgument("Index exceeds bucket capacity");
//     }
    
//     // Calculate the byte offset in bucket_data_ (each slot is 4 bytes)
//     unsigned byte_offset = index * SLOT_SIZE;
    
//     // Write the offset value to the corresponding slot
//     // Store as 4-byte unsigned integer
//     memcpy(bucket_data_ + byte_offset, &offset, SLOT_SIZE);
    
//     return Status::OK();
// }

Status BucketTable::get(const Slice& key, unsigned& offset) {
    uint64_t key_index = 0;
    if (!key.TryGetIndex(&key_index) || key_index < min_key_index_ || key_index >= max_key_index_) {
        return Status::InvalidArgument("BucketTable: key is not a valid key in this bucket");
    }
    const uint64_t idx = key_index - min_key_index_;
    if (idx >= SLOT_NUM_PER_BUCKET) {
        return Status::InvalidArgument("BucketTable: key exceeds bucket capacity");
    }
    
    const uint64_t byte_offset = idx * SLOT_SIZE;
    
    ibv_mr remote_mr = bucket_mr_;
    remote_mr.addr = static_cast<void*>(static_cast<char*>(bucket_mr_.addr) + byte_offset);
    
    ibv_mr* local_mr = rdma_mg_->Get_local_read_mr();
    
    int ret = rdma_mg_->RDMA_Read(&remote_mr, local_mr, SLOT_SIZE, 
                                  "default", IBV_SEND_SIGNALED, 1, 0);
    
    if (ret != 0) {
        return Status::IOError("RDMA Read failed");
    }
    
    memcpy(&offset, local_mr->addr, SLOT_SIZE);
    
    return Status::OK();
}

}
