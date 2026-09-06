#ifndef BUCKET_TABLE_H
#define BUCKET_TABLE_H

#include <include/TimberSaw/slice.h>
#include <include/TimberSaw/status.h>
#include <include/TimberSaw/options.h>
#include <util/rdma.h>
#include <memory>


namespace TimberSaw {

/*  
    Bucket Table for (Key, offset) pairs.
        bucket_data_[key] has the offset of the value in value zone.
        Every offset is SLOT_SIZE bytes, so the bucket can store BUCKET_SIZE/SLOT_SIZE pairs.
     The bucket table is stored in RDMA memory, using bucket_mr_.
*/
class BucketTable {
public:
    BucketTable(Slice min_key, Slice max_key, std::shared_ptr<RDMA_Manager> rdma_mg);
    BucketTable(Slice min_key, Slice max_key, const ibv_mr& bucket_mr, std::shared_ptr<RDMA_Manager> rdma_mg);
    ~BucketTable();

    BucketTable(const BucketTable&) = delete;
    BucketTable& operator=(const BucketTable&) = delete;
    BucketTable(BucketTable&&) = delete;
    BucketTable& operator=(BucketTable&&) = delete;

    Status get(const Slice& key, unsigned& offset);
    const ibv_mr& bucket_mr() const { return bucket_mr_; }

private:
    char* bucket_data_;
    uint64_t min_key_index_ = 0;
    uint64_t max_key_index_ = 0;
    std::shared_ptr<RDMA_Manager> rdma_mg_;
    ibv_mr bucket_mr_;
    bool owns_remote_memory_ = false;
};

}



#endif  // BUCKET_TABLE_H
