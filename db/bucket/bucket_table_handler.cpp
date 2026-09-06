#include "bucket_table.h"
#include "bucket_table_handler.h"

namespace TimberSaw {

TimberSaw::Status BucketTableHandler::Get(Slice& Key) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t key_index = 0;
    if (!Key.TryGetIndex(&key_index) || key_index >= max_key_range_) {
        return TimberSaw::Status::InvalidArgument("BucketTableHandler: invalid numeric key");
    }
    const auto region_idx = key_index / SLOT_NUM_PER_BUCKET;
    auto map_it = region_idx_to_bucket_idx_.find(region_idx);
    if (map_it == region_idx_to_bucket_idx_.end()) {
        return TimberSaw::Status::NotFound("BucketTableHandler: Region not mapped to bucket");
    }
    const auto bucket_idx = map_it->second;

    unsigned offset;
    Status s = bucket_table_[bucket_idx]->get(Key, offset);
    if (!s.ok()) {
        return s;
    }

    if (offset == 0) {
        return TimberSaw::Status::NotFound("BucketTableHandler: Key not found in bucket table");
    } else {
        return TimberSaw::Status::OK();
    }
    
}

TimberSaw::Status BucketTableHandler::GetOffsetForRegionKey(size_t region_idx, const Slice& key, uint64_t* offset_units) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (offset_units == nullptr) {
        return TimberSaw::Status::InvalidArgument("BucketTableHandler: offset output is null");
    }
    auto map_it = region_idx_to_bucket_idx_.find(region_idx);
    if (map_it == region_idx_to_bucket_idx_.end()) {
        return TimberSaw::Status::NotFound("BucketTableHandler: Region not mapped to bucket");
    }

    const auto bucket_idx = map_it->second;
    Slice key_copy(key);
    unsigned offset = 0;
    Status s = bucket_table_[bucket_idx]->get(key_copy, offset);
    if (!s.ok()) {
        return s;
    }
    if (offset == 0) {
        return TimberSaw::Status::NotFound("BucketTableHandler: Key not found in bucket table");
    }
    *offset_units = offset;
    return TimberSaw::Status::OK();
}
}
