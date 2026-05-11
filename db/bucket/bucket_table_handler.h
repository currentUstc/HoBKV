#ifndef BUCKET_TABLE_HANDLER

#define BUCKET_TABLE_HANDLER

#include "bucket_table.h"
#include <vector>
#include <unordered_map>
#include <include/TimberSaw/status.h>
#include <include/TimberSaw/slice.h>

namespace TimberSaw {
class BucketTableHandler {
public:
    BucketTableHandler(size_t max_num, unsigned long long max_key_range) : 
        max_num_(max_num), current_num_(0), max_key_range_(max_key_range) {}

    size_t get_max_num() const { return max_num_; }
    size_t get_current_num() const { return current_num_; }

    TimberSaw::Status AddBucketTable(std::shared_ptr<TimberSaw::BucketTable> bt) {
        if (current_num_ >= max_num_) {
            return TimberSaw::Status::InvalidArgument("BucketTableHandler: Exceeded maximum number of bucket tables");
        }
        // 是否在该函数内创建 BucketTable 对象？
        bucket_table_.push_back(*bt);
        current_num_++;
        return TimberSaw::Status::OK();
    }

    TimberSaw::Status AssignRegionToNewBucket(size_t region_idx, const std::shared_ptr<TimberSaw::BucketTable>& bt) {
        if (region_idx_to_bucket_idx_.find(region_idx) != region_idx_to_bucket_idx_.end()) {
            return TimberSaw::Status::OK();
        }
        TimberSaw::Status s = AddBucketTable(bt);
        if (!s.ok()) {
            return s;
        }
        region_idx_to_bucket_idx_[region_idx] = current_num_ - 1;
        return TimberSaw::Status::OK();
    }
    bool HasBucketForRegion(size_t region_idx) const {
        return region_idx_to_bucket_idx_.find(region_idx) != region_idx_to_bucket_idx_.end();
    }
    TimberSaw::Status GetBucketMRForRegion(size_t region_idx, ibv_mr* bucket_mr) const {
        if (bucket_mr == nullptr) {
            return TimberSaw::Status::InvalidArgument("BucketTableHandler: bucket_mr is null");
        }
        auto map_it = region_idx_to_bucket_idx_.find(region_idx);
        if (map_it == region_idx_to_bucket_idx_.end()) {
            return TimberSaw::Status::NotFound("BucketTableHandler: Region not mapped to bucket");
        }
        const auto bucket_idx = map_it->second;
        if (bucket_idx >= bucket_table_.size()) {
            return TimberSaw::Status::Corruption("BucketTableHandler: bucket index out of range");
        }
        *bucket_mr = bucket_table_[bucket_idx].bucket_mr();
        return TimberSaw::Status::OK();
    }

    TimberSaw::Status Get(Slice& Key);
    TimberSaw::Status GetOffsetForRegionKey(size_t region_idx, const Slice& key, uint64_t* offset_units);

    ~BucketTableHandler() {
        for (auto bt : bucket_table_) {
            // No need to delete since using shared_ptr
        }
    }

private:
    std::vector<TimberSaw::BucketTable> bucket_table_;

    size_t max_num_;
    size_t current_num_;

    unsigned long long max_key_range_ = 0;
    std::unordered_map<size_t, size_t> region_idx_to_bucket_idx_;

};

}

#endif  // BUCKET_TABLE_HANDLER