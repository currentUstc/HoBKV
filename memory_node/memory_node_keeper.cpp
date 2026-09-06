//
// Created by ruihong on 7/29/21.
//
#include "memory_node/memory_node_keeper.h"

#include "db/filename.h"
#include "db/bucket/bucket_table_builder_memoryside.h"
#include "db/table_cache.h"
#include "TimberSaw/options.h"
#include "TimberSaw/status.h"
#include <cstdio>
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <list>
#include <limits>
#include <numa.h>
#include <unordered_set>

#include "table/table_builder_bams.h"
#include "table/table_builder_memoryside.h"
#include "util/coding.h"

namespace TimberSaw {

namespace {

bool CopyFromValueZone(const std::vector<ibv_mr*>& value_zone_mrs, uint64_t byte_offset,
                       char* dst, size_t size) {
    size_t copied = 0;
    while (copied < size) {
        const size_t mr_index = byte_offset / VALUE_ZONE_ALLOC_SIZE;
        const size_t offset_in_mr = byte_offset % VALUE_ZONE_ALLOC_SIZE;
        if (mr_index >= value_zone_mrs.size()) {
            return false;
        }
        const ibv_mr* mr = value_zone_mrs[mr_index];
        if (mr == nullptr || mr->addr == nullptr || offset_in_mr >= mr->length) {
            return false;
        }
        const size_t chunk = std::min(size - copied, mr->length - offset_in_mr);
        std::memcpy(dst + copied, static_cast<const char*>(mr->addr) + offset_in_mr, chunk);
        copied += chunk;
        byte_offset += chunk;
    }
    return true;
}

bool CopyToValueZone(const std::vector<ibv_mr*>& value_zone_mrs, uint64_t byte_offset,
                     const char* src, size_t size) {
    size_t copied = 0;
    while (copied < size) {
        const size_t mr_index = byte_offset / VALUE_ZONE_ALLOC_SIZE;
        const size_t offset_in_mr = byte_offset % VALUE_ZONE_ALLOC_SIZE;
        if (mr_index >= value_zone_mrs.size()) {
            return false;
        }
        ibv_mr* mr = value_zone_mrs[mr_index];
        if (mr == nullptr || mr->addr == nullptr || offset_in_mr >= mr->length) {
            return false;
        }
        const size_t chunk = std::min(size - copied, mr->length - offset_in_mr);
        std::memcpy(static_cast<char*>(mr->addr) + offset_in_mr, src + copied, chunk);
        copied += chunk;
        byte_offset += chunk;
    }
    return true;
}

// Decode an entry by its logical value-zone offset.  An entry may cross a
// 1 GB MR boundary, so assemble it from consecutive value-zone MRs first.
bool TryExtractValueFromValueZone(const std::vector<ibv_mr*>& value_zone_mrs, uint64_t offset_units,
                                  uint64_t expected_key_index, std::string* key, std::string* value,
                                  SequenceNumber* sequence, ValueType* value_type,
                                  uint64_t* previous_offset_units) {
    if (key == nullptr || value == nullptr || sequence == nullptr || value_type == nullptr) {
        return false;
    }

    const uint64_t byte_offset = offset_units * VALUE_ZONE_OFFSET_UNIT;
    char header[48];
    if (!CopyFromValueZone(value_zone_mrs, byte_offset, header, sizeof(header))) {
        return false;
    }
    const uint64_t total_size = DecodeFixed64(header);
    if (total_size < 56 || total_size > (64ull * 1024ull * 1024ull)) {
        return false;
    }

    std::string record(static_cast<size_t>(total_size), '\0');
    if (!CopyFromValueZone(value_zone_mrs, byte_offset, record.data(), record.size())) {
        return false;
    }
    const char* base = record.data();
    if (DecodeFixed64(base + 8) != expected_key_index) {
        return false;
    }
    const uint64_t raw_type = DecodeFixed64(base + 24);
    const uint64_t key_size = DecodeFixed64(base + 32);
    const uint64_t value_size = DecodeFixed64(base + 40);
    const size_t padded_key_size =
        (static_cast<size_t>(key_size) + VALUE_ZONE_OFFSET_UNIT - 1) / VALUE_ZONE_OFFSET_UNIT *
        VALUE_ZONE_OFFSET_UNIT;
    const size_t padded_value_size =
        (static_cast<size_t>(value_size) + VALUE_ZONE_OFFSET_UNIT - 1) / VALUE_ZONE_OFFSET_UNIT *
        VALUE_ZONE_OFFSET_UNIT;
    const size_t raw_total_size = 48 + padded_key_size + padded_value_size + 8;
    if (raw_total_size > record.size() || record.size() - raw_total_size >= VALUE_ZONE_OFFSET_UNIT) {
        return false;
    }
    key->assign(base + 48, static_cast<size_t>(key_size));
    value->assign(base + 48 + padded_key_size, static_cast<size_t>(value_size));
    *sequence = DecodeFixed64(base + 16);
    *value_type = raw_type == 0 ? kTypeValue : kTypeDeletion;
    if (previous_offset_units != nullptr) {
        *previous_offset_units = DecodeFixed32(base + 48 + padded_key_size + padded_value_size + 4);
    }
    return true;
}

// Link an unpublished value-zone record to the previous bucket head. The
// caller publishes the new slot only after this function succeeds.
bool PatchValueZonePreviousOffset(const std::vector<ibv_mr*>& value_zone_mrs,
                                  uint64_t offset_units, uint64_t expected_key_index,
                                  SequenceNumber expected_sequence, uint64_t previous_offset_units) {
    if (previous_offset_units > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    const uint64_t byte_offset = offset_units * VALUE_ZONE_OFFSET_UNIT;
    char header[48];
    if (!CopyFromValueZone(value_zone_mrs, byte_offset, header, sizeof(header)) ||
        DecodeFixed64(header + 8) != expected_key_index ||
        DecodeFixed64(header + 16) != expected_sequence) {
        return false;
    }
    const uint64_t total_size = DecodeFixed64(header);
    const uint64_t key_size = DecodeFixed64(header + 32);
    const uint64_t value_size = DecodeFixed64(header + 40);
    const size_t padded_key_size =
        (static_cast<size_t>(key_size) + VALUE_ZONE_OFFSET_UNIT - 1) / VALUE_ZONE_OFFSET_UNIT *
        VALUE_ZONE_OFFSET_UNIT;
    const size_t padded_value_size =
        (static_cast<size_t>(value_size) + VALUE_ZONE_OFFSET_UNIT - 1) / VALUE_ZONE_OFFSET_UNIT *
        VALUE_ZONE_OFFSET_UNIT;
    const size_t raw_total_size = 48 + padded_key_size + padded_value_size + 8;
    if (total_size < raw_total_size || total_size - raw_total_size >= VALUE_ZONE_OFFSET_UNIT) {
        return false;
    }
    char encoded_previous[sizeof(uint32_t)];
    EncodeFixed32(encoded_previous, static_cast<uint32_t>(previous_offset_units));
    const uint64_t previous_position = byte_offset + 48 + padded_key_size + padded_value_size + 4;
    return CopyToValueZone(value_zone_mrs, previous_position, encoded_previous,
                           sizeof(encoded_previous));
}

std::string BuildInternalKey(const std::string& user_key, SequenceNumber sequence, ValueType value_type) {
    ParsedInternalKey pik(Slice(user_key), sequence, value_type);
    std::string internal_key;
    AppendInternalKey(&internal_key, pik);
    return internal_key;
}

bool TryParseKeyIndex(const Slice& user_key, uint64_t* key_index) {
    if (key_index == nullptr || user_key.empty()) {
        return false;
    }
    std::string key_str(user_key.data(), user_key.size());
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(key_str.c_str(), &end, 10);
    if (errno != 0 || end == key_str.c_str() || *end != '\0') {
        return false;
    }
    *key_index = static_cast<uint64_t>(parsed);
    return true;
}

}  // namespace

std::shared_ptr<RDMA_Manager> Memory_Node_Keeper::rdma_mg = std::shared_ptr<RDMA_Manager>();
TimberSaw::Memory_Node_Keeper::Memory_Node_Keeper(bool use_sub_compaction, uint32_t tcp_port, int pr_s)
    : pr_size(pr_s),
      opts(std::make_shared<Options>(true)),
      internal_comparator_(BytewiseComparator()),
      mmap_limiter_(MaxMmaps()),
      fd_limiter_(MaxOpenFiles()),
      descriptor_file(nullptr),
      descriptor_log(nullptr),
      usesubcompaction(use_sub_compaction),
#if TABLE_STRATEGY == 2
      table_cache_(new TableCache("home_node", *opts, opts->max_table_cache_size)),
#else
      table_cache_(new TableCache("home_node", *opts, opts->max_open_files)),
#endif
      versions_(new VersionSet("home_node", opts.get(), table_cache_, &internal_comparator_, &mtx_temp)) {
    struct TimberSaw::config_t config = {NULL,     /* dev_name */
                                         NULL,     /* server_name */
                                         tcp_port, /* tcp_port */
                                         1,        /* ib_port */
                                         1,        /* gid_idx */
                                         0};
    //  size_t write_block_size = 4*1024*1024;
    //  size_t read_block_size = 4*1024;
    size_t table_size = 10 * 1024 * 1024;
    rdma_mg = std::make_shared<RDMA_Manager>(config, table_size);  // set memory server node id as 1.
    //    rdma_mg = new RDMA_Manager(config, table_size);
    rdma_mg->Mempool_initialize(FlushBuffer, RDMA_WRITE_BLOCK, 0);
    if (opts->compression != kNoCompression) {
        rdma_mg->Mempool_initialize(FilterChunk, FILTER_BLOCK_COMPRESSION, 0);
    } else {
        rdma_mg->Mempool_initialize(FilterChunk, FILTER_BLOCK, 0);
    }
    rdma_mg->Mempool_initialize(IndexChunk, INDEX_BLOCK_BIG, 0);
    rdma_mg->Mempool_initialize(IndexChunk_Small, INDEX_BLOCK_SMALL, 0);
    // Bucket/value-zone allocation requests use distinct chunk sizes and must
    // be registered in the memory-side allocator before the first hot shard.
    rdma_mg->Mempool_initialize(BucketChunk, BUCKET_SIZE, 0);
    rdma_mg->Mempool_initialize(ValueZoneChunk, VALUE_ZONE_ALLOC_SIZE, 0);

    //    rdma_mg->Mempool_initialize(FlushBuffer, RDMA_WRITE_BLOCK, 0);
    // TODO: actually we don't need Prefetch buffer.
    //    rdma_mg->Mempool_initialize(std::string("Prefetch"), RDMA_WRITE_BLOCK);
    // TODO: add a handle function for the option value to get the non-default bloombits.
    opts->filter_policy = new InternalFilterPolicy(NewBloomFilterPolicy(opts->bloom_bits));
    opts->comparator = &internal_comparator_;
    //    ClipToRange(&opts->max_open_files, 64 + kNumNonTableCacheFiles, 50000);
    //    ClipToRange(&opts->write_buffer_size, 64 << 10, 1 << 30);
    //    ClipToRange(&opts->max_file_size, 1 << 20, 1 << 30);
    //    ClipToRange(&opts->block_size, 1 << 10, 4 << 20);

#ifdef PERFECT_THREAD_NUMBER_FOR_BGTHREADS
    int available_cpu_num = numa_num_task_cpus();
    // The background thread should be larger than the actual core number in compute node,
    //  because the bg thread trigger the near data compaction will sleep.Acurately speaking,
    //  the compute background thread number = compute core number  + memory core number
    Compactor_pool_.SetBackgroundThreads(available_cpu_num);
    opts->MaxSubcompaction = available_cpu_num;
#else
    Compactor_pool_.SetBackgroundThreads(opts->max_background_compactions);
#endif
    Message_handler_pool_.SetBackgroundThreads(2);
    Persistency_bg_pool_.SetBackgroundThreads(1);

    // Set up the connection information.
    std::string connection_conf;
    size_t pos = 0;
    std::ifstream myfile;
    myfile.open(config_file_name, std::ios_base::in);
    std::string space_delimiter = " ";

    std::getline(myfile, connection_conf);
    uint8_t i = 0;
    uint8_t id;
    while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
        id = 2 * i + 1;
        rdma_mg->compute_nodes.insert({id, connection_conf.substr(0, pos)});
        connection_conf.erase(0, pos + space_delimiter.length());
        i++;
    }
    rdma_mg->compute_nodes.insert({2 * i + 1, connection_conf});
    assert((rdma_mg->node_id - 1) / 2 < rdma_mg->compute_nodes.size());
    i = 0;
    std::getline(myfile, connection_conf);
    while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
        id = 2 * i;
        rdma_mg->memory_nodes.insert({id, connection_conf.substr(0, pos)});
        connection_conf.erase(0, pos + space_delimiter.length());
        i++;
    }
    rdma_mg->memory_nodes.insert({2 * i, connection_conf});
    i++;
}

Memory_Node_Keeper::~Memory_Node_Keeper() {
    delete opts->filter_policy;
    if (descriptor_log != nullptr) {
        delete descriptor_log;
    }
    if (descriptor_file != nullptr) {
        delete descriptor_file;
    }
}

Status Memory_Node_Keeper::RegisterBucketTable(uint64_t bucket_idx, char* bucket_data, size_t bucket_bytes) {
    if (bucket_data == nullptr || bucket_bytes == 0) {
        return Status::InvalidArgument("RegisterBucketTable: bucket data is null");
    }
    std::lock_guard<std::mutex> lock(bucket_table_mu_);
    if (bucket_table_builders_.find(bucket_idx) != bucket_table_builders_.end()) {
        return Status::OK();
    }
    bucket_table_builders_[bucket_idx] =
        std::make_shared<BucketTableBuilderMemorySide>(bucket_data, bucket_bytes);
    return Status::OK();
}

std::shared_ptr<BucketTableBuilderMemorySide> Memory_Node_Keeper::GetBucketTableBuilder(uint64_t bucket_idx) {
    std::lock_guard<std::mutex> lock(bucket_table_mu_);
    auto it = bucket_table_builders_.find(bucket_idx);
    if (it == bucket_table_builders_.end()) {
        return nullptr;
    }
    return it->second;
}

void Memory_Node_Keeper::RefreshBucketBitmap(BucketMigrationMeta* meta) {
    if (meta == nullptr) {
        return;
    }
    if (meta->subrange_bitmap.size() != meta->subrange_overlap_counts.size()) {
        meta->subrange_bitmap.assign(meta->subrange_overlap_counts.size(), 0);
    }
    for (size_t i = 0; i < meta->subrange_overlap_counts.size(); ++i) {
        meta->subrange_bitmap[i] = meta->subrange_overlap_counts[i] > 0 ? 1 : 0;
    }
}

void Memory_Node_Keeper::ApplyFileRangeDeltaToMeta(BucketMigrationMeta* meta, uint64_t file_min_key, uint64_t file_max_key,
                                                   int delta) {
    if (meta == nullptr || meta->max_key <= meta->min_key || meta->subrange_overlap_counts.empty()) {
        return;
    }
    if (file_max_key < meta->min_key || file_min_key >= meta->max_key) {
        return;
    }

    const uint64_t overlap_start = std::max(meta->min_key, file_min_key);
    const uint64_t overlap_end_inclusive = std::min(meta->max_key - 1, file_max_key);
    if (overlap_start > overlap_end_inclusive) {
        return;
    }

    const uint64_t total_span = meta->max_key - meta->min_key;
    const uint64_t subrange_size =
        std::max<uint64_t>(1, (total_span + meta->subrange_overlap_counts.size() - 1) / meta->subrange_overlap_counts.size());

    size_t start_idx = static_cast<size_t>((overlap_start - meta->min_key) / subrange_size);
    size_t end_idx = static_cast<size_t>((overlap_end_inclusive - meta->min_key) / subrange_size);
    if (start_idx >= meta->subrange_overlap_counts.size()) {
        start_idx = meta->subrange_overlap_counts.size() - 1;
    }
    if (end_idx >= meta->subrange_overlap_counts.size()) {
        end_idx = meta->subrange_overlap_counts.size() - 1;
    }

    for (size_t i = start_idx; i <= end_idx; ++i) {
        if (delta > 0) {
            meta->subrange_overlap_counts[i] += static_cast<uint32_t>(delta);
        } else if (delta < 0) {
            const uint32_t d = static_cast<uint32_t>(-delta);
            meta->subrange_overlap_counts[i] = meta->subrange_overlap_counts[i] > d ? meta->subrange_overlap_counts[i] - d : 0;
        }
    }
}

void Memory_Node_Keeper::RecomputeBucketMigrationMeta(BucketMigrationMeta* meta) {
    if (meta == nullptr || meta->max_key <= meta->min_key) {
        return;
    }
    meta->subrange_overlap_counts.assign(kBucketSubrangeCount, 0);
    meta->subrange_bitmap.assign(kBucketSubrangeCount, 0);

    InternalKey begin_internal(std::to_string(meta->min_key), kMaxSequenceNumber, kTypeValue);
    InternalKey end_internal(std::to_string(meta->max_key - 1), 0, kTypeDeletion);
    std::unordered_set<std::string> visited_files;

    std::lock_guard<std::mutex> lock(versionset_mtx);
    Version* current = versions_->current();
    for (int level = 0; level < config::kNumLevels; ++level) {
        std::vector<std::shared_ptr<RemoteMemTableMetaData>> inputs;
        current->GetOverlappingInputs(level, &begin_internal, &end_internal, &inputs);
        for (const auto& file : inputs) {
            if (!file) {
                continue;
            }
            const std::string unique_file_id =
                std::to_string(level) + ":" + std::to_string(file->number) + ":" + std::to_string(file->creator_node_id);
            if (!visited_files.insert(unique_file_id).second) {
                continue;
            }
            uint64_t file_min_key = 0;
            uint64_t file_max_key = 0;
            if (TryParseKeyIndex(file->smallest.user_key(), &file_min_key) &&
                TryParseKeyIndex(file->largest.user_key(), &file_max_key)) {
                ApplyFileRangeDeltaToMeta(meta, file_min_key, file_max_key, +1);
            } else {
                for (auto& count : meta->subrange_overlap_counts) {
                    ++count;
                }
            }
        }
    }
    RefreshBucketBitmap(meta);
}

void Memory_Node_Keeper::SendBucketMetaUpdateToCompute(const BucketMigrationMeta& meta, const std::string& client_ip,
                                                       uint8_t target_node_id) {
    std::lock_guard<std::mutex> main_send_lock(rdma_mg->main_send_cq_mutex);
    ibv_mr send_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Request* send_pointer = reinterpret_cast<RDMA_Request*>(send_mr.addr);
    *send_pointer = {};
    send_pointer->command = bucket_meta_update;
    send_pointer->content.bucket_meta_update.shard_idx = meta.shard_idx;
    send_pointer->content.bucket_meta_update.bucket_idx = meta.bucket_idx;
    send_pointer->content.bucket_meta_update.min_key = meta.min_key;
    send_pointer->content.bucket_meta_update.max_key = meta.max_key;
    send_pointer->content.bucket_meta_update.cutover_sequence = meta.cutover_sequence;
    const uint32_t subrange_count =
        static_cast<uint32_t>(std::min<size_t>(meta.subrange_bitmap.size(), kBucketMetaBitmapMax));
    send_pointer->content.bucket_meta_update.subrange_count = subrange_count;
    std::memset(send_pointer->content.bucket_meta_update.subrange_bitmap, 0, kBucketMetaBitmapMax);
    for (uint32_t i = 0; i < subrange_count; ++i) {
        send_pointer->content.bucket_meta_update.subrange_bitmap[i] = meta.subrange_bitmap[i];
    }
    rdma_mg->post_send<RDMA_Request>(&send_mr, target_node_id, client_ip);
    ibv_wc wc[2] = {};
    if (rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id)) {
        std::fprintf(stderr, "bucket_meta_update: send failed, shard=%llu\n",
                     static_cast<unsigned long long>(meta.shard_idx));
    }
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
}

void Memory_Node_Keeper::UpdateBucketMigrationMetaAfterCompaction(Compaction* compaction, const std::string& client_ip,
                                                                  uint8_t target_node_id) {
    if (compaction == nullptr) {
        return;
    }
    std::vector<BucketMigrationMeta> updates_to_sync;
    {
        std::lock_guard<std::mutex> meta_lock(cold_to_hot_meta_mu_);
        if (bucket_migration_meta_by_shard_.empty()) {
            return;
        }

        for (auto& kv : bucket_migration_meta_by_shard_) {
            BucketMigrationMeta& meta = kv.second;

            for (int which = 0; which < 2; ++which) {
                for (int i = 0; i < compaction->num_input_files(which); ++i) {
                    const auto& file = compaction->input(which, i);
                    if (!file) {
                        continue;
                    }
                    uint64_t file_min_key = 0;
                    uint64_t file_max_key = 0;
                    if (TryParseKeyIndex(file->smallest.user_key(), &file_min_key) &&
                        TryParseKeyIndex(file->largest.user_key(), &file_max_key)) {
                        ApplyFileRangeDeltaToMeta(&meta, file_min_key, file_max_key, -1);
                    } else {
                        RecomputeBucketMigrationMeta(&meta);
                    }
                }
            }

            if (compaction->edit() != nullptr) {
                auto* new_files = compaction->edit()->GetNewFiles();
                for (const auto& level_file : *new_files) {
                    const auto& file = level_file.second;
                    if (!file) {
                        continue;
                    }
                    uint64_t file_min_key = 0;
                    uint64_t file_max_key = 0;
                    if (TryParseKeyIndex(file->smallest.user_key(), &file_min_key) &&
                        TryParseKeyIndex(file->largest.user_key(), &file_max_key)) {
                        ApplyFileRangeDeltaToMeta(&meta, file_min_key, file_max_key, +1);
                    } else {
                        RecomputeBucketMigrationMeta(&meta);
                    }
                }
            }

            RefreshBucketBitmap(&meta);
            updates_to_sync.push_back(meta);
        }
    }
    for (const auto& meta : updates_to_sync) {
        SendBucketMetaUpdateToCompute(meta, client_ip, target_node_id);
    }
}

//  void TimberSaw::Memory_Node_Keeper::Schedule(void (*background_work_function)(void*),
//                                             void* background_work_arg,
//                                             ThreadPoolType type) {
//    message_handler_pool_.Schedule(background_work_function, background_work_arg);
//  }
void Memory_Node_Keeper::SetBackgroundThreads(int num, ThreadPoolType type) {
    Compactor_pool_.SetBackgroundThreads(num);
}
//  void Memory_Node_Keeper::MaybeScheduleCompaction(std::string& client_ip) {
//    if (versions_->NeedsCompaction()) {
//      //    background_compaction_scheduled_ = true;
//      printf("Need a compaction.\n");
//      void* function_args = new std::string(client_ip);
//      BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = function_args};
//      if (Compactor_pool_.queue_len_.load()>256){
//        //If there has already be enough compaction scheduled, then drop this one
//        printf("queue length has been too long %d elements in the queue\n", Compactor_pool_.queue_len_.load());
//        return;
//      }
//      Compactor_pool_.Schedule(BGWork_Compaction, static_cast<void*>(thread_pool_args));
//      printf("Schedule a Compaction !\n");
//    }
//  }
//  void Memory_Node_Keeper::BGWork_Compaction(void* thread_arg) {
//    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_arg);
//    ((Memory_Node_Keeper*)p->db)->BackgroundCompaction(p->func_args);
//    delete static_cast<BGThreadMetadata*>(thread_arg);
//  }
void Memory_Node_Keeper::RPC_Compaction_Dispatch(void* thread_args) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
    ((Memory_Node_Keeper*)p->db)->sst_compaction_handler(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_args);
}
void Memory_Node_Keeper::RPC_Garbage_Collection_Dispatch(void* thread_args) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
    ((Memory_Node_Keeper*)p->db)->sst_garbage_collection(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_args);
}
void Memory_Node_Keeper::RPC_Flush_Helper_Dispatch(void* thread_args) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
    ((Memory_Node_Keeper*)p->db)->flush_helper_handler(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_args);
}
// hot_to_cold 消息分发入口。
void Memory_Node_Keeper::RPC_Hot_To_Cold_Dispatch(void* thread_args) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
    ((Memory_Node_Keeper*)p->db)->hot_to_cold_handler(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_args);
}
// cold_to_hot 消息分发入口。
void Memory_Node_Keeper::RPC_Cold_To_Hot_Dispatch(void* thread_args) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
    ((Memory_Node_Keeper*)p->db)->cold_to_hot_handler(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_args);
}
void Memory_Node_Keeper::RPC_Bucket_Scan_Dispatch(void* thread_args) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
    ((Memory_Node_Keeper*)p->db)->bucket_scan_handler(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_args);
}
void Memory_Node_Keeper::Persistence_Dispatch(void* thread_args) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_args);
    ((Memory_Node_Keeper*)p->db)->PersistSSTables(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_args);
}

//  void Memory_Node_Keeper::BackgroundCompaction(void* p) {
//  //  write_stall_mutex_.AssertNotHeld();
//  std::string* client_ip = static_cast<std::string*>(p);
//  if (versions_->NeedsCompaction()) {
//    Compaction* c;
////    bool is_manual = (manual_compaction_ != nullptr);
////    InternalKey manual_end;
////    if (is_manual) {
////      ManualCompaction* m = manual_compaction_;
////      c = versions_->CompactRange(m->level, m->begin, m->end);
////      m->done = (c == nullptr);
////      if (c != nullptr) {
////        manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
////      }
////      Log(options_.info_log,
////          "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
////          m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
////          (m->end ? m->end->DebugString().c_str() : "(end)"),
////          (m->done ? "(end)" : manual_end.DebugString().c_str()));
////    } else {
//      c = versions_->PickCompaction();
//      //if there is no task to pick up, just return.
//      if (c== nullptr){
//        DEBUG("compaction task executed but not found doable task.\n");
//        delete c;
//        return;
//      }
//
////    }
//    //    write_stall_mutex_.AssertNotHeld();
//    Status status;
//    if (c == nullptr) {
//      // Nothing to do
//    } else if (c->IsTrivialMove()) {
//      // Move file to next level
//      assert(c->num_input_files(0) == 1);
//      std::shared_ptr<RemoteMemTableMetaData> f = c->input(0, 0);
//      c->edit()->RemoveFile(c->level(), f->number, f->creator_node_id);
//      c->edit()->AddFile(c->level() + 1, f);
//      f->level = f->level +1;
//      {
////        std::unique_lock<std::mutex> l(versions_mtx);// TODO(ruihong): remove all the superversion mutex usage.
//        c->ReleaseInputs();
//        {
//          std::unique_lock<std::mutex> lck(versionset_mtx);
//          status = versions_->LogAndApply(c->edit(), 0);
//          versions_->Pin_Version_For_Compute();
//          Edit_sync_to_remote(c->edit(), *client_ip, &lck);
//        }
////        InstallSuperVersion();
//      }
//
//      DEBUG("Trival compaction\n");
//    } else {
//      CompactionState* compact = new CompactionState(c);
// #ifndef NDEBUG
//      if (c->level() >= 1){
//        printf("Compaction level > 1");
//      }
// #endif
//      auto start = std::chrono::high_resolution_clock::now();
//      //      write_stall_mutex_.AssertNotHeld();
//      // Only when there is enough input level files and output level files will the subcompaction triggered
//      if (usesubcompaction && c->num_input_files(0)>=4 && c->num_input_files(1)>1){
//        status = DoCompactionWorkWithSubcompaction(compact, *client_ip);
////        status = DoCompactionWork(compact, *client_ip);
//      }else{
//        status = DoCompactionWork(compact, *client_ip);
//      }
//
//      auto stop = std::chrono::high_resolution_clock::now();
//      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
////#ifndef NDEBUG
//      printf("Table compaction time elapse (%ld) us, compaction level is %d, first level file number %d, the second
//      level file number %d \n",
//             duration.count(), compact->compaction->level(),
//             compact->compaction->num_input_files(0),compact->compaction->num_input_files(1) );
////#endif
//      DEBUG("Non-trivalcompaction!\n");
//      std::cout << "compaction task table number in the first level"<<compact->compaction->inputs_[0].size() <<
//      std::endl; if (!status.ok()) {
//        std::cerr << "compaction failed" << std::endl;
////        RecordBackgroundError(status);
//      }
//      CleanupCompaction(compact);
//      //    RemoveObsoleteFiles();
//    }
//    delete c;
//
////    if (status.ok()) {
////      // Done
////    } else if (shutting_down_.load(std::memory_order_acquire)) {
////      // Ignore compaction errors found during shutting down
////    } else {
////      Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
////    }
//
////    if (is_manual) {
////      ManualCompaction* m = manual_compaction_;
////      if (!status.ok()) {
////        m->done = true;
////      }
////      if (!m->done) {
////        // We only compacted part of the requested range.  Update *m
////        // to the range that is left to be compacted.
////        m->tmp_storage = manual_end;
////        m->begin = &m->tmp_storage;
////      }
////      manual_compaction_ = nullptr;
////    }
//  }
////  MaybeScheduleCompaction(*client_ip);
//  delete client_ip;
//
//}
void Memory_Node_Keeper::PersistSSTables(void* arg) {
    VersionEdit_Merger* edit_merger = ((Arg_for_persistent*)arg)->edit_merger;
    std::string client_ip = ((Arg_for_persistent*)arg)->client_ip;
    uint8_t target_node_id = ((Arg_for_persistent*)arg)->target_node_id;
    //  if(!ve_merger.merge_one_edit(edit_merger)){
    //    // NOt digesting enough edit, directly get the next edit.
    //
    //    // unpin the sstables merged during the edit merge
    //    if (ve_merger.ready_to_upin_merged_file){
    //      UnpinSSTables_RPC(&ve_merger.merged_file_numbers, client_ip);
    //      ve_merger.ready_to_upin_merged_file = false;
    //      ve_merger.merged_file_numbers.clear();
    //    }
    //    return;
    //  }else{
    //
    //    // unpin the sstables merged during the edit merge
    //    if (ve_merger.ready_to_upin_merged_file){
    //      UnpinSSTables_RPC(&ve_merger.merged_file_numbers, client_ip);
    //      ve_merger.ready_to_upin_merged_file = false;
    //      ve_merger.merged_file_numbers.clear();
    //    }

    // The version edit merger has merge enough edits, lets make those files durable.
    DEBUG("A work pesistent work request was executed--------------\n");
    assert(edit_merger->GetNewFilesNum() > 0);
//    uint64_t thread_number = edit_merger->GetNewFilesNum() - edit_merger->only_trival_change.size();
#ifndef NDEBUG
    // assert all the file number in the only trival change list exist in the ve_merger
    //  so that we can calculate the thread number correctly.
    for (auto iter : edit_merger->only_trival_change) {
        assert(edit_merger->GetNewFiles()->find(iter) != edit_merger->GetNewFiles()->end());
    }
#endif
    //    if (!edit_merger->IsTrival()){
    DEBUG_arg("Persist the files&&&&&&&&&&&&&&&&&&&&&, file number is %zu\n", edit_merger->GetNewFiles()->size());
    // TODO: We also need to delete those file in edit_merger->deleted_files. Otherwise there will be disk space leak.
    //      std::thread* threads = new std::thread[thread_number];
    //      int i = 0;
    for (const auto& iter : *edit_merger->GetNewFiles()) {
        // do not persist the sstable of trival move. Is the thread over-provisioned?
        if (edit_merger->only_trival_change.find(iter.first) == edit_merger->only_trival_change.end()) {
            // we do not need multiple threads for the checkpointing.
            PersistSSTable(iter.second);

            //          threads[i]= std::thread(&Memory_Node_Keeper::PersistSSTable, this, iter.second);
            //          i++;
        }
        // Clean up the obsoleted files below.
    }
    //      assert(i == thread_number);
    //      for (int j = 0; j < thread_number; ++j) {
    //        threads[j].join();
    //      }
    //      delete[] threads;
    //    }

    // Initialize new descriptor log file if necessary by creating
    // a temporary file that contains a snapshot of the current version.
    std::string new_manifest_file;
    Status s;
    if (descriptor_log == nullptr) {
        // No reason to unlock *mu here since we only hit this path in the
        // first call to LogAndApply (when opening the database).
        assert(descriptor_file == nullptr);
        new_manifest_file = DescriptorFileName("./db_content", manifest_file_number_);
        //      edit->SetNextFile(next_file_number_);

        s = NewWritableFile(new_manifest_file, &descriptor_file);
        if (s.ok()) {
            descriptor_log = new log::Writer(descriptor_file);
            //        s = WriteSnapshot(descriptor_log);
        }
    }
    // Write new record to MANIFEST log
    if (s.ok()) {
        std::string record;
        edit_merger->EncodeToDiskFormat(&record);
        s = descriptor_log->AddRecord(record);
        if (s.ok()) {
            s = descriptor_file->Sync();
        }
    }
    check_point_t_ready.store(true);
    //    if (!edit_merger->IsTrival()){
    DEBUG("Unpin the SSTables *$$$$$$$$$$$$$$$$$$$\n");
    UnpinSSTables_RPC(edit_merger, client_ip, target_node_id);
    //    }
    //    ve_merger.Clear();
    delete edit_merger;
}
void Memory_Node_Keeper::PersistSSTable(std::shared_ptr<RemoteMemTableMetaData> sstable_ptr) {
    DEBUG_arg("Persist SSTable %lu", sstable_ptr->number);
    std::string fname = TableFileName("./db_content", sstable_ptr->number);
    WritableFile* f;
    //      std::vector<uint32_t> chunk_barriers;
    uint32_t barrier_size = sstable_ptr->remote_data_mrs.size() + sstable_ptr->remote_dataindex_mrs.size() +
                            sstable_ptr->remote_filter_mrs.size();
    uint32_t* barrier_arr = new uint32_t[barrier_size];
    Status s = NewWritableFile(fname, &f);
    assert(f);
    uint32_t offset = 0;
    uint32_t chunk_index = 0;
    for (auto chunk : sstable_ptr->remote_data_mrs) {
        offset += chunk.second->length;
        barrier_arr[chunk_index] = offset;
        f->Append(Slice((char*)chunk.second->addr, chunk.second->length));
        chunk_index++;
    }

    for (auto chunk : sstable_ptr->remote_dataindex_mrs) {
        offset += chunk.second->length;
        barrier_arr[chunk_index] = offset;
        f->Append(Slice((char*)chunk.second->addr, chunk.second->length));
        chunk_index++;
    }
    for (auto chunk : sstable_ptr->remote_filter_mrs) {
        offset += chunk.second->length;
        barrier_arr[chunk_index] = offset;
        f->Append(Slice((char*)chunk.second->addr, chunk.second->length));
        chunk_index++;
    }
    f->Append(Slice((char*)barrier_arr, barrier_size * sizeof(uint32_t)));

    f->Append(Slice((char*)&barrier_size, sizeof(uint32_t)));
    f->Flush();
    f->Sync();
    f->Close();
    delete[] barrier_arr;
}

void Memory_Node_Keeper::UnpinSSTables_RPC(VersionEdit_Merger* edit_merger, std::string& client_ip,
                                           uint8_t target_node_id) {
    RDMA_Request* send_pointer;
    ibv_mr send_mr = {};
    ibv_mr send_mr_large = {};
    ibv_mr receive_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr_large, Version_edit);

    rdma_mg->Allocate_Local_RDMA_Slot(receive_mr, Message);

    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    uint64_t* arr_ptr = (uint64_t*)send_mr_large.addr;
    uint32_t index = 0;
    bool empty = edit_merger->only_trival_change.empty();
    assert(edit_merger->GetNewFilesNum() > 0);
    for (auto iter : *edit_merger->GetNewFiles()) {
        // Exclude the trival file from the unpin.
        if (!empty &&
            edit_merger->only_trival_change.find(iter.second->number) != edit_merger->only_trival_change.end()) {
            continue;
        }
#ifndef NDEBUG
        //    DEBUG_arg("Unpin file number is %lu, id 1 ****************\n", iter.second->number);
        assert(edit_merger->debug_map.find(iter.second->number) == edit_merger->debug_map.end());
        edit_merger->debug_map.insert(iter.second->number);
#endif
        arr_ptr[index] = iter.second->number;
        index++;
    }
    assert(index * sizeof(uint64_t) < send_mr_large.length);
    memset((char*)send_mr_large.addr + index * sizeof(uint64_t), 1, 1);
    send_pointer = (RDMA_Request*)send_mr.addr;
    send_pointer->command = persist_unpin_;
    send_pointer->content.psu.buffer_size = index * sizeof(uint64_t) + 1;
    send_pointer->content.psu.id = 1;
    send_pointer->buffer = receive_mr.addr;
    send_pointer->rkey = receive_mr.rkey;
    RDMA_Reply* receive_pointer;
    receive_pointer = (RDMA_Reply*)receive_mr.addr;
    // Clear the reply buffer for the polling.
    *receive_pointer = {};
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->post_send<RDMA_Request>(&send_mr, target_node_id, client_ip);
    // TODO: Think of a better way to avoid deadlock and guarantee the same
    //  sequence of verision edit between compute node and memory server.
    ibv_wc wc[2] = {};
    if (rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id)) {
        fprintf(stderr, "failed to poll send for remote memory register\n");
        return;
    }

    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    if (!rdma_mg->poll_reply_buffer(receive_pointer))  // poll the receive for 2 entires
    {
        printf("Reply buffer is %p", receive_pointer->buffer);
        printf("Received is %d", receive_pointer->received);
        printf("receive structure size is %lu", sizeof(RDMA_Reply));
        printf("version id is %lu", versions_->version_id);
        exit(0);
    }
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->RDMA_Write(receive_pointer->buffer_large, receive_pointer->rkey_large, &send_mr_large,
                        index * sizeof(uint64_t) + 1, client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr_large.addr, Version_edit);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    rdma_mg->Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
}

void Memory_Node_Keeper::UnpinSSTables_RPC(std::list<uint64_t>* merged_file_number, std::string& client_ip,
                                           uint8_t target_node_id) {
    RDMA_Request* send_pointer;
    ibv_mr send_mr = {};
    ibv_mr send_mr_large = {};
    ibv_mr receive_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr_large, Version_edit);

    rdma_mg->Allocate_Local_RDMA_Slot(receive_mr, Message);

    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    uint64_t* arr_ptr = (uint64_t*)send_mr_large.addr;
    uint32_t index = 0;
    DEBUG("Unpin RPC for merged files\n");
    for (auto iter : *merged_file_number) {
#ifndef NDEBUG
        //    DEBUG_arg("Unpin file number is %lu, id 2 ****************\n", iter);
        assert(ve_merger.debug_map.find(iter) == ve_merger.debug_map.end());
        ve_merger.debug_map.insert(iter);
#endif
        arr_ptr[index] = iter;
        index++;
    }
    memset((char*)send_mr_large.addr + index * sizeof(uint64_t), 1, 1);

    send_pointer = (RDMA_Request*)send_mr.addr;
    send_pointer->command = persist_unpin_;
    send_pointer->content.psu.buffer_size = index * sizeof(uint64_t) + 1;
    send_pointer->content.psu.id = 2;
    send_pointer->buffer = receive_mr.addr;
    send_pointer->rkey = receive_mr.rkey;
    RDMA_Reply* receive_pointer;
    receive_pointer = (RDMA_Reply*)receive_mr.addr;
    // Clear the reply buffer for the polling.
    *receive_pointer = {};
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->post_send<RDMA_Request>(&send_mr, target_node_id, client_ip);
    // TODO: Think of a better way to avoid deadlock and guarantee the same
    //  sequence of verision edit between compute node and memory server.
    ibv_wc wc[2] = {};
    if (rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id)) {
        fprintf(stderr, "failed to poll send for remote memory register\n");
        return;
    }

    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    if (!rdma_mg->poll_reply_buffer(receive_pointer))  // poll the receive for 2 entires
    {
        printf("Reply buffer is %p", receive_pointer->buffer);
        printf("Received is %d", receive_pointer->received);
        printf("receive structure size is %lu", sizeof(RDMA_Reply));
        printf("version id is %lu", versions_->version_id);
        exit(0);
    }
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->RDMA_Write(receive_pointer->buffer_large, receive_pointer->rkey_large, &send_mr_large,
                        index * sizeof(uint64_t) + 1, client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr_large.addr, Version_edit);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    rdma_mg->Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
}
void Memory_Node_Keeper::CleanupCompaction(CompactionState* compact) {
    //  undefine_mutex.AssertHeld();
    if (compact->builder != nullptr) {
        // May happen if we get a shutdown call in the middle of compaction
        compact->builder->Abandon();
        delete compact->builder;
    } else {
        //    assert(compact->outfile == nullptr);
    }
    //  delete compact->outfile;
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionOutput& out = compact->outputs[i];
        //    pending_outputs_.erase(out.number);
    }
    delete compact;
}
Status Memory_Node_Keeper::DoCompactionWork(CompactionState* compact, std::string& client_ip) {
    //  const uint64_t start_micros = env_->NowMicros();
    int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

    //  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
    assert(compact->builder == nullptr);
    //  assert(compact->outfile == nullptr);
    //  if (snapshots_.empty()) {
    //    compact->smallest_snapshot = versions_->LastSequence();
    //  } else {
    //    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
    //  }

    Iterator* input = versions_->MakeInputIteratorMemoryServer(compact->compaction);

    // Release mutex while we're actually doing the compaction work
    //  undefine_mutex.Unlock();

    input->SeekToFirst();
#ifndef NDEBUG
    int Not_drop_counter = 0;
    int number_of_key = 0;
#endif
    Status status;
    // TODO: try to create two ikey for parsed key, they can in turn represent the current user key
    //  and former one, which can save the data copy overhead.
    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    Slice key;
    assert(input->Valid());
#ifndef NDEBUG
    printf("first key is %s", input->key().ToString().c_str());
#endif
    while (input->Valid()) {
        key = input->key();
        //    assert(key.data()[0] == '0');
        // We do not need to check whether the output file have too much overlap with level n + 2.
        // If there is a lot of overlap subcompaction can be triggered.
        // compact->compaction->ShouldStopBefore(key) &&
        //    if (compact->builder != nullptr) {
        //      status = FinishCompactionOutputFile(compact, input);
        //      if (!status.ok()) {
        //        break;
        //      }
        //    }
        // key merged below!!!
        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key) {
                // TODO: can we avoid the data copy here, can we set two buffers in block and make
                //  the old user key not be garbage collected so that the old Slice can be
                //  directly used here.
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
            } else if (user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                //        has_current_user_key = true;
                //        last_sequence_for_key = kMaxSequenceNumber;
                // this will result in the key not drop, next if will always be false because of
                // the last_sequence_for_key.
            } else {
                drop = true;
            }
        }
#ifndef NDEBUG
        number_of_key++;
#endif
        if (!drop) {
            // Open output file if necessary
            if (compact->builder == nullptr) {
                status = OpenCompactionOutputFile(compact);
                if (!status.ok()) {
                    break;
                }
            }
            if (compact->builder->NumEntries() == 0) {
                compact->current_output()->smallest.DecodeFrom(key);
            }
#ifndef NDEBUG
            Not_drop_counter++;
#endif
            compact->builder->Add(key, input->value());
            //      assert(key.data()[0] == '0');
            // Close output file if it is big enough
            if (compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
                //        assert(key.data()[0] == '0');
                compact->current_output()->largest.DecodeFrom(key);
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }
        }
        //    assert(key.data()[0] == '0');
        //    input->Next();
        //    if(*key.data() != 0){
        //      printf("break here");
        //    }
        input->Next();
        //    if(*key.data() != 0){
        //      printf("break here");
        //    }
        // NOTE(ruihong): When the level iterator is invalid it will be deleted and then the key will
        // be invalid also.
        //    assert(key.data()[0] == '0');
    }
    //  reinterpret_cast<TimberSaw::MergingIterator>
    // You can not call prev here because the iterator is not valid any more
    //  input->Prev();
    //  assert(input->Valid());
#ifndef NDEBUG
    printf("For compaction, Total number of key touched is %d, KV left is %d\n", number_of_key, Not_drop_counter);
#endif
    //  assert(key.data()[0] == '0');
    //  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    //    status = Status::IOError("Deleting DB during compaction");
    //  }
    if (status.ok() && compact->builder != nullptr) {
        //    assert(key.data()[0] == '0');
        compact->current_output()->largest.DecodeFrom(key);
        status = FinishCompactionOutputFile(compact, input);
    }
    if (status.ok()) {
        status = input->status();
    }
    delete input;
    input = nullptr;

    CompactionStats stats;
    //  stats.micros = env_->NowMicros() - start_micros - imm_micros;
    //  for (int which = 0; which < 2; which++) {
    //    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
    //      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    //    }
    //  }
    //  for (size_t i = 0; i < compact->outputs.size(); i++) {
    //    stats.bytes_written += compact->outputs[i].file_size;
    //  }
    // TODO: we can remove this lock.
    //  undefine_mutex.Lock();
    //  stats_[compact->compaction->level() + 1].Add(stats);

    //  if (status.ok()) {
    ////    std::unique_lock<std::mutex> l(versions_mtx, std::defer_lock);
    //    status = InstallCompactionResults(compact, client_ip);
    ////    InstallSuperVersion();
    //  }
    //  undefine_mutex.Unlock();
    //  if (!status.ok()) {
    //    RecordBackgroundError(status);
    //  }
    //  VersionSet::LevelSummaryStorage tmp;
    //  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
    // NOtifying all the waiting threads.
    //  write_stall_cv.notify_all();
    //  Versionset_Sync_To_Compute(VersionEdit* ve);
    return status;
}
Status Memory_Node_Keeper::DoCompactionWorkWithSubcompaction(CompactionState* compact, std::string& client_ip) {
    Compaction* c = compact->compaction;
    // TODO need to check the snapeshot in the compute node. Or modify the logic in get()
    c->GenSubcompactionBoundaries();
    auto boundaries = c->GetBoundaries();
    auto sizes = c->GetSizes();
    assert(boundaries->size() == sizes->size() - 1);
    //  int subcompaction_num = std::min((int)c->GetBoundariesNum(), config::MaxSubcompaction);
    if (boundaries->size() <= opts->MaxSubcompaction) {
        for (size_t i = 0; i <= boundaries->size(); i++) {
            Slice* start = i == 0 ? nullptr : &(*boundaries)[i - 1];
            Slice* end = i == boundaries->size() ? nullptr : &(*boundaries)[i];
            compact->sub_compact_states.emplace_back(c, start, end, (*sizes)[i]);
        }
    } else {
        // Get output level total file size.
        uint64_t sum = c->GetFileSizesForLevel(1);
        std::list<int> small_files{};
        for (int i = 0; i < sizes->size(); i++) {
            if ((*sizes)[i] <= opts->max_file_size / 4) small_files.push_back(i);
        }
        int big_files_num = boundaries->size() - small_files.size();
        int files_per_subcompaction =
            big_files_num / opts->MaxSubcompaction + 1;  // Due to interger round down, we need add 1.
        double mean = sum * 1.0 / opts->MaxSubcompaction;
        for (size_t i = 0; i <= boundaries->size(); i++) {
            size_t range_size = (*sizes)[i];
            Slice* start = i == 0 ? nullptr : &(*boundaries)[i - 1];
            int files_counter = range_size <= opts->max_file_size / 4 ? 0 : 1;  // count this file.
            // TODO(Ruihong) make a better strategy to group the boundaries.
            // Version 1
            //      while (i!=boundaries->size() && range_size < mean &&
            //             range_size + (*sizes)[i+1] <= mean + 3*options_.max_file_size/4){
            //        i++;
            //        range_size += (*sizes)[i];
            //      }
            // Version 2
            while (i != boundaries->size() &&
                   (files_counter < files_per_subcompaction || (*sizes)[i + 1] <= opts->max_file_size / 4)) {
                i++;
                size_t this_file_size = (*sizes)[i];
                range_size += this_file_size;
                // Only increase the file counter when add big file.
                if (this_file_size >= opts->max_file_size / 4) files_counter++;
            }
            Slice* end = i == boundaries->size() ? nullptr : &(*boundaries)[i];
            compact->sub_compact_states.emplace_back(c, start, end, range_size);
        }
    }
    printf("Subcompaction number is %zu", compact->sub_compact_states.size());
    const size_t num_threads = compact->sub_compact_states.size();
    assert(num_threads > 0);
    //  const uint64_t start_micros = env_->NowMicros();

    // Launch a thread for each of subcompactions 1...num_threads-1
    std::vector<port::Thread> thread_pool;
    thread_pool.reserve(num_threads - 1);
    for (size_t i = 1; i < compact->sub_compact_states.size(); i++) {
        thread_pool.emplace_back(&Memory_Node_Keeper::ProcessKeyValueCompaction, this, &compact->sub_compact_states[i]);
    }

    // Always schedule the first subcompaction (whether or not there are also
    // others) in the current thread to be efficient with resources
    ProcessKeyValueCompaction(&compact->sub_compact_states[0]);
    for (auto& thread : thread_pool) {
        thread.join();
    }
    //  CompactionStats stats;
    ////  stats.micros = env_->NowMicros() - start_micros;
    //  for (int which = 0; which < 2; which++) {
    //    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
    //      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    //    }
    //  }
    //  for (auto iter : compact->sub_compact_states) {
    //    for (size_t i = 0; i < iter.outputs.size(); i++) {
    //      stats.bytes_written += iter.outputs[i].file_size;
    //    }
    //  }

    // TODO: we can remove this lock.

    Status status = Status::OK();
    //  {
    ////    std::unique_lock<std::mutex> l(superversion_mtx, std::defer_lock);
    //    status = InstallCompactionResults(compact, client_ip);
    ////    InstallSuperVersion();
    //  }

    //  if (!status.ok()) {
    //    RecordBackgroundError(status);
    //  }
    //  VersionSet::LevelSummaryStorage tmp;
    //  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
    //  // NOtifying all the waiting threads.
    //  write_stall_cv.notify_all();
    return status;
}
void Memory_Node_Keeper::ProcessKeyValueCompaction(SubcompactionState* sub_compact) {
    assert(sub_compact->builder == nullptr);
    // Start and End are userkeys.
    Slice* start = sub_compact->start;
    Slice* end = sub_compact->end;
    //  if (snapshots_.empty()) {
    //    sub_compact->smallest_snapshot = versions_->LastSequence();
    //  } else {
    //    sub_compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
    //  }

    Iterator* input = versions_->MakeInputIteratorMemoryServer(sub_compact->compaction);

    // Release mutex while we're actually doing the compaction work
    //  undefine_mutex.Unlock();

    if (start != nullptr) {
        // The compaction range is (start, end]. so we set 0 as look up key sequence.
        InternalKey start_internal(*start, 0, kValueTypeForSeek);
        // tofix(ruihong): too much data copy for the seek here!
        input->Seek(start_internal.Encode());
        Slice temp = input->key();
        assert(internal_comparator_.Compare(temp, *start) > 0);
        // The first key larger or equal to start_internal was covered in the subtask before it.
        // The range for the subcompactions are (s1,e1] (s2,e2] ... (sn,en]
        input->Next();
    } else {
        input->SeekToFirst();
    }
#ifndef NDEBUG
    int Not_drop_counter = 0;
    int number_of_key = 0;
#endif
    Status status;
    // TODO: try to create two ikey for parsed key, they can in turn represent the current user key
    //  and former one, which can save the data copy overhead.
    ParsedInternalKey ikey;
    std::string current_user_key;

    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    Slice key;
    assert(input->Valid());
#ifndef NDEBUG
    std::string last_internal_key;
    printf("first key is %s", input->key().ToString().c_str());
#endif
    while (input->Valid()) {
        key = input->key();
        assert(key.ToString() != last_internal_key);
#ifndef NDEBUG
        if (start) {
            assert(internal_comparator_.Compare(key, *start) > 0);
        }
#endif
        //    assert(key.data()[0] == '0');
        // We do not need to check whether the output file have too much overlap with level n + 2.
        // If there is a lot of overlap subcompaction can be triggered.
        // compact->compaction->ShouldStopBefore(key) &&
        //    if (sub_compact->builder != nullptr) {
        //
        //      sub_compact->current_output()->largest.SetFrom(ikey);
        //      status = FinishCompactionOutputFile(sub_compact, input);
        //      if (!status.ok()) {
        //        DEBUG("Should stop status not OK\n");
        //        break;
        //      }
        //    }
        // TODO: record the largest key as the last ikey, find a more efficient way to record
        // the last key of SSTable.

        // key merged below!!!
        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key) {
                // TODO: can we avoid the data copy here, can we set two buffers in block and make
                //  the old user key not be garbage collected so that the old Slice can be
                //  directly used here.
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
#ifndef NDEBUG
                last_internal_key = key.ToString();
#endif
                has_current_user_key = true;
            } else if (user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
#ifndef NDEBUG
                last_internal_key = key.ToString();
#endif
                //        has_current_user_key = true;
                //        last_sequence_for_key = kMaxSequenceNumber;
                // this will result in the key not drop, next if will always be false because of
                // the last_sequence_for_key.
            } else {
                drop = true;
            }
        }
#ifndef NDEBUG
        number_of_key++;
#endif
        if (!drop) {
            // Open output file if necessary
            if (sub_compact->builder == nullptr) {
                status = OpenCompactionOutputFile(sub_compact);
                if (!status.ok()) {
                    break;
                }
            }
            if (sub_compact->builder->NumEntries() == 0) {
                //        assert(key.data()[0] == '\000');
                sub_compact->current_output()->smallest.DecodeFrom(key);
            }
#ifndef NDEBUG
            Not_drop_counter++;
#endif
            sub_compact->builder->Add(key, input->value());
            //      assert(key.data()[0] == '0');
            // Close output file if it is big enough
            if (sub_compact->builder->FileSize() >= sub_compact->compaction->MaxOutputFileSize()) {
                //        assert(key.data()[0] == '\000');
                sub_compact->current_output()->largest.DecodeFrom(key);
                assert(!sub_compact->current_output()->largest.Encode().ToString().empty());

                assert(internal_comparator_.Compare(sub_compact->current_output()->largest,
                                                    sub_compact->current_output()->smallest) > 0);
                status = FinishCompactionOutputFile(sub_compact, input);
                if (!status.ok()) {
                    DEBUG("Iterator status is not OK\n");
                    break;
                }
            }
        }
        if (end != nullptr && user_comparator()->Compare(ExtractUserKey(key), *end) >= 0) {
            assert(user_comparator()->Compare(ExtractUserKey(key), *end) == 0);
            break;
        }
        //    assert(key.data()[0] == '0');
        input->Next();
        // NOTE(ruihong): When the level iterator is invalid it will be deleted and then the key will
        //  be invalid also.
        //     assert(key.data()[0] == '0');
    }
    //  reinterpret_cast<TimberSaw::MergingIterator>
    // You can not call prev here because the iterator is not valid any more
    //  input->Prev();
    //  assert(input->Valid());
#ifndef NDEBUG
    printf("For compaction, Total number of key touched is %d, KV left is %d\n", number_of_key, Not_drop_counter);
#endif
    if (status.ok() && sub_compact->builder != nullptr) {
        //    assert(key.size()>0);
        //    assert(key.data()[0] == '\000');
        sub_compact->current_output()->largest.DecodeFrom(
            key);  // The SSTable for subcompaction range will be (start, end]
        assert(!sub_compact->current_output()->largest.Encode().ToString().empty());
        status = FinishCompactionOutputFile(sub_compact, input);
    }
    if (status.ok()) {
        status = input->status();
    }
    delete input;
    //  input = nullptr;
}
Status Memory_Node_Keeper::OpenCompactionOutputFile(SubcompactionState* compact) {
    assert(compact != nullptr);
    assert(compact->builder == nullptr);
    uint64_t file_number;
    {
        //    undefine_mutex.Lock();
        file_number = versions_->NewFileNumber();
        //    pending_outputs_.insert(file_number);
        CompactionOutput out;
        out.number = file_number;
        out.smallest.Clear();
        out.largest.Clear();
        compact->outputs.push_back(out);
        //    undefine_mutex.Unlock();
    }

    // Make the output file
    //  std::string fname = TableFileName(dbname_, file_number);
    //  Status s = env_->NewWritableFile(fname, &compact->outfile);
    Status s = Status::OK();
    if (s.ok()) {
        if (compact->compaction->table_type == block_based) {
            compact->builder = new TableBuilder_Memoryside(*opts, Compact, rdma_mg);
        } else {
            compact->builder = new TableBuilder_BAMS(*opts, Compact, rdma_mg);
        }
    }
    return s;
}
Status Memory_Node_Keeper::OpenCompactionOutputFile(CompactionState* compact) {
    assert(compact != nullptr);
    assert(compact->builder == nullptr);
    uint64_t file_number;
    {
        //    undefine_mutex.Lock();
        // No need to allocate the file number here.
        //    file_number = versions_->NewFileNumber();
        //    pending_outputs_.insert(file_number);
        CompactionOutput out;
        //    out.number = file_number;
        out.smallest.Clear();
        out.largest.Clear();
        compact->outputs.push_back(out);
        //    undefine_mutex.Unlock();
    }

    // Make the output file
    //  std::string fname = TableFileName(dbname_, file_number);
    //  Status s = env_->NewWritableFile(fname, &compact->outfile);
    Status s = Status::OK();
    if (s.ok()) {
        if (compact->compaction->table_type == block_based) {
            compact->builder = new TableBuilder_Memoryside(*opts, Compact, rdma_mg);
        } else {
            compact->builder = new TableBuilder_BAMS(*opts, Compact, rdma_mg);
        }
    }
    //  printf("rep_ is %p", compact->builder->get_filter_map())
    return s;
}
Status Memory_Node_Keeper::FinishCompactionOutputFile(SubcompactionState* compact, Iterator* input) {
    assert(compact != nullptr);
    //  assert(compact->outfile != nullptr);
    assert(compact->builder != nullptr);

    const uint64_t output_number = compact->current_output()->number;
    assert(!compact->current_output()->largest.Encode().empty());
#ifndef NDEBUG
    //  if (output_number == 11 ||output_number == 12 ){
    //    printf("Finish Compaction output number is 11 or 12\n");
    printf("File number %lu largest key size is %lu", compact->current_output()->number,
           compact->current_output()->largest.Encode().ToString().size());
//  }
#endif
    assert(output_number != 0);

    // Check for iterator errors
    Status s = input->status();
    const uint64_t current_entries = compact->builder->NumEntries();
    if (s.ok()) {
        s = compact->builder->Finish();
    } else {
        printf("iterator Error!!!!!!!!!!!, Error: %s\n", s.ToString().c_str());
        compact->builder->Abandon();
    }

    compact->builder->get_datablocks_map(compact->current_output()->remote_data_mrs);
    compact->builder->get_dataindexblocks_map(compact->current_output()->remote_dataindex_mrs);
    compact->builder->get_filter_map(compact->current_output()->remote_filter_mrs);
#ifndef NDEBUG
    uint64_t file_size = 0;
    for (auto iter : compact->current_output()->remote_data_mrs) {
        file_size += iter.second->length;
    }
#endif
    const uint64_t current_bytes = compact->builder->FileSize();
    compact->current_output()->file_size = current_bytes;
    assert(file_size == current_bytes);
    compact->total_bytes += current_bytes;
    delete compact->builder;
    compact->builder = nullptr;

    // Finish and check for file errors
    //  if (s.ok()) {
    //    s = compact->outfile->Sync();
    //  }
    //  if (s.ok()) {
    //    s = compact->outfile->Close();
    //  }
    //  delete compact->outfile;
    //  compact->outfile = nullptr;

    if (s.ok() && current_entries > 0) {
        // Verify that the table is usable
        //    Iterator* iter =
        //        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
        //    s = iter->status();
        //    delete iter;
    }
    return s;
}
Status Memory_Node_Keeper::FinishCompactionOutputFile(CompactionState* compact, Iterator* input) {
    assert(compact != nullptr);
    //  assert(compact->outfile != nullptr);
    assert(compact->builder != nullptr);

    const uint64_t output_number = compact->current_output()->number;
    //  assert(output_number != 0);

    // Check for iterator errors
    Status s = input->status();
    const uint64_t current_entries = compact->builder->NumEntries();
    if (s.ok()) {
        s = compact->builder->Finish();
    } else {
        printf("iterator Error!!!!!!!!!!!, Error: %s\n", s.ToString().c_str());
        compact->builder->Abandon();
    }

    compact->builder->get_datablocks_map(compact->current_output()->remote_data_mrs);
    compact->builder->get_dataindexblocks_map(compact->current_output()->remote_dataindex_mrs);
    compact->builder->get_filter_map(compact->current_output()->remote_filter_mrs);
    assert(compact->current_output()->remote_data_mrs.size() > 0);
    assert(compact->current_output()->remote_dataindex_mrs.size() > 0);
#ifndef NDEBUG
    uint64_t file_size = 0;
    for (auto iter : compact->current_output()->remote_data_mrs) {
        file_size += iter.second->length;
    }
#endif
    const uint64_t current_bytes = compact->builder->FileSize();
    compact->current_output()->file_size = current_bytes;
    assert(file_size == current_bytes);
    compact->total_bytes += current_bytes;
    delete compact->builder;
    compact->builder = nullptr;

    // Finish and check for file errors
    //  if (s.ok()) {
    //    s = compact->outfile->Sync();
    //  }
    //  if (s.ok()) {
    //    s = compact->outfile->Close();
    //  }
    //  delete compact->outfile;
    //  compact->outfile = nullptr;

    if (s.ok() && current_entries > 0) {
        // Verify that the table is usable
        //    Iterator* iter =
        //        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
        //    s = iter->status();
        //    delete iter;
        //    if (s.ok()) {
        //      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
        //          (unsigned long long)output_number, compact->compaction->level(),
        //          (unsigned long long)current_entries,
        //          (unsigned long long)current_bytes);
        //    }
    }
    return s;
}
Status Memory_Node_Keeper::InstallCompactionResults(CompactionState* compact, std::string& client_ip) {
    //  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
    //      compact->compaction->num_input_files(0), compact->compaction->level(),
    //      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
    //      static_cast<long long>(compact->total_bytes));

    // Add compaction outputs
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();
    if (compact->sub_compact_states.size() == 0) {
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionOutput& out = compact->outputs[i];
            if (out.remote_data_mrs.empty() || out.remote_dataindex_mrs.empty()) {
                continue;
            }
            std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(1);
            meta->shard_target_node_id = rdma_mg->node_id;
            // TODO make all the metadata written into out
            meta->number = out.number;
            meta->file_size = out.file_size;
            meta->level = level + 1;
            meta->smallest = out.smallest;
            assert(!out.largest.Encode().ToString().empty());
            meta->largest = out.largest;
            meta->remote_data_mrs = out.remote_data_mrs;
            meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
            meta->remote_filter_mrs = out.remote_filter_mrs;
            compact->compaction->edit()->AddFile(level + 1, meta);
            assert(!meta->UnderCompaction);
#ifndef NDEBUG

            // Verify that the table is usable
            Iterator* it = versions_->table_cache_->NewIterator_MemorySide(ReadOptions(), meta);
            //        s = it->status();

            it->SeekToFirst();
            while (it->Valid()) {
                it->Next();
            }
            printf("Table %p Read successfully after compaction\n", meta.get());
            delete it;
#endif
        }
    } else {
        for (auto subcompact : compact->sub_compact_states) {
            for (size_t i = 0; i < subcompact.outputs.size(); i++) {
                const CompactionOutput& out = subcompact.outputs[i];
                if (out.remote_data_mrs.empty() || out.remote_dataindex_mrs.empty()) {
                    continue;
                }
                std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(1);
                meta->shard_target_node_id = rdma_mg->node_id;  // this id is important.
                // TODO make all the metadata written into out
                meta->number = out.number;
                meta->file_size = out.file_size;
                meta->level = level + 1;
                meta->smallest = out.smallest;
                meta->largest = out.largest;
                meta->remote_data_mrs = out.remote_data_mrs;
                meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
                meta->remote_filter_mrs = out.remote_filter_mrs;
                compact->compaction->edit()->AddFile(level + 1, meta);
                assert(!meta->UnderCompaction);
            }
        }
    }
    assert(compact->compaction->edit()->GetNewFilesNum() > 0);
    //  lck_p->lock();
    compact->compaction->ReleaseInputs();
    std::unique_lock<std::mutex> lck(versionset_mtx, std::defer_lock);

    Status s = versions_->LogAndApply(compact->compaction->edit());
    //  versions_->Pin_Version_For_Compute();

    Edit_sync_to_remote(compact->compaction->edit(), client_ip, &lck, 0);

    return s;
}

Status Memory_Node_Keeper::InstallCompactionResultsToComputePreparation(CompactionState* compact) {
    //  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
    //      compact->compaction->num_input_files(0), compact->compaction->level(),
    //      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
    //      static_cast<long long>(compact->total_bytes));

    // Add compaction outputs
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();
    assert(level >= 0);
    if (compact->sub_compact_states.size() == 0) {
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionOutput& out = compact->outputs[i];
            if (out.remote_data_mrs.empty() || out.remote_dataindex_mrs.empty()) {
                continue;
            }
            std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(1);
            meta->shard_target_node_id = rdma_mg->node_id;  // this id is important.
            // TODO make all the metadata written into out
            //      meta->number = out.number;
            meta->file_size = out.file_size;
            meta->level = level + 1;
            meta->smallest = out.smallest;
            assert(!out.largest.Encode().ToString().empty());
            meta->largest = out.largest;
            meta->remote_data_mrs = out.remote_data_mrs;
            meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
            meta->remote_filter_mrs = out.remote_filter_mrs;
            meta->table_type = compact->compaction->table_type;
            compact->compaction->edit()->AddFile(level + 1, meta);
            assert(!meta->UnderCompaction);
        }
    } else {
        for (auto subcompact : compact->sub_compact_states) {
            for (size_t i = 0; i < subcompact.outputs.size(); i++) {
                const CompactionOutput& out = subcompact.outputs[i];
                if (out.remote_data_mrs.empty() || out.remote_dataindex_mrs.empty()) {
                    continue;
                }
                std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(1);
                meta->shard_target_node_id = rdma_mg->node_id;  // this id is important.
                // TODO make all the metadata written into out
                //        meta->number = out.number;
                meta->file_size = out.file_size;
                meta->level = level + 1;
                meta->smallest = out.smallest;
                meta->largest = out.largest;
                meta->remote_data_mrs = out.remote_data_mrs;
                meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
                meta->remote_filter_mrs = out.remote_filter_mrs;
                meta->table_type = compact->compaction->table_type;
                compact->compaction->edit()->AddFile(level + 1, meta);
                assert(!meta->UnderCompaction);
            }
        }
    }
    assert(compact->compaction->edit()->GetNewFilesNum() > 0);
    Status s = Status::OK();
    //  lck_p->lock();
    //  compact->compaction->ReleaseInputs();
    //  std::unique_lock<std::mutex> lck(versionset_mtx);
    //  Status s = versions_->LogAndApply(compact->compaction->edit(), 0);
    //  versions_->Pin_Version_For_Compute();

    //  Edit_sync_to_remote(compact->compaction->edit(), client_ip, &lck);

    return s;
}
void Memory_Node_Keeper::server_communication_thread(std::string client_ip, int socket_fd) {
    printf("A new shared memory thread start\n");
    printf("checkpoint-out\n");
    char temp_receive[2];
    char temp_send[] = "Q";
    int rc = 0;
    uint8_t compute_node_id;
    rdma_mg->ConnectQPThroughSocket(client_ip, socket_fd, compute_node_id);

    printf("The connected compute node's id is %d\n", compute_node_id);
    rdma_mg->res->sock_map.insert({compute_node_id, socket_fd});
    // TODO: use Local_Memory_Allocation to bulk allocate, and assign within this function.
    //    ibv_mr send_mr[32] = {};
    //    for(int i = 0; i<32; i++){
    //      rdma_mg->Allocate_Local_RDMA_Slot(send_mr[i], "message");
    //    }

    //    char* send_buff;
    //    if (!rdma_mg_->Local_Memory_Register(&send_buff, &send_mr, 1000, std::string())) {
    //      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
    //    }
    //    int buffer_number = 32;
    ibv_mr recv_mr[R_SIZE] = {};
    for (int i = 0; i < R_SIZE; i++) {
        rdma_mg->Allocate_Local_RDMA_Slot(recv_mr[i], Message);
    }

    //    char* recv_buff;
    //    if (!rdma_mg_->Local_Memory_Register(&recv_buff, &recv_mr, 1000, std::string())) {
    //      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
    //    }
    //  post_receive<int>(recv_mr, client_ip);
    printf("checkpoint3\n");
    for (int i = 0; i < R_SIZE; i++) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[i], compute_node_id, client_ip);
    }
    //    rdma_mg_->post_receive(recv_mr, client_ip, sizeof(Computing_to_memory_msg));
    // sync after send & recv buffer creation and receive request posting.
    rdma_mg->local_mem_pool.reserve(100);
    if (rdma_mg->pre_allocated_pool.size() < pr_size) {
        std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
        rdma_mg->Preregister_Memory(pr_size);
    }
    if (rdma_mg->sock_sync_data(socket_fd, 1, temp_send, temp_receive)) /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error after QPs are were moved to RTS\n");
        rc = 1;
    }
    //      shutdown(socket_fd, 2);
    //    close(socket_fd);
    //  post_send<int>(res->mr_send, client_ip);
    ibv_wc wc[3] = {};
    rdma_mg->connection_counter.fetch_add(1);
    //    std::thread* thread_sync;
    printf("\ncheckpoint4\n");
    if (rdma_mg->connection_counter.load() == rdma_mg->compute_nodes.size() && rdma_mg->node_id == 0) {
        std::thread thread_sync(&RDMA_Manager::sync_with_computes_Mside, rdma_mg.get());
        // Need to be detached.
        thread_sync.detach();
    }
    //  if(poll_completion(wc, 2, client_ip))
    //    printf("The main qp not create correctly");
    //  else
    //    printf("The main qp not create correctly");
    // Computing node and share memory connection succeed.
    // Now is the communication through rdma.

    //  receive_msg_buf = (computing_to_memory_msg*)recv_buff;
    //  receive_msg_buf->command = ntohl(receive_msg_buf->command);
    //  receive_msg_buf->content.qp_config.qp_num = ntohl(receive_msg_buf->content.qp_config.qp_num);
    //  receive_msg_buf->content.qp_config.lid = ntohs(receive_msg_buf->content.qp_config.lid);
    //  ibv_wc wc[3] = {};

    //
    // TODO: implement a heart beat mechanism.

    // directly start the heart beat from the remote memory side, when all the connection to
    // the compute nodes are ready.
    if (rdma_mg->connection_counter.load() == rdma_mg->compute_nodes.size()) {
        create_cpu_util_heart_beater_sender();
    }

    int buffer_position = 0;
    int miss_poll_counter = 0;
    printf("checkpoint5\n");
    while (true) {
        ++miss_poll_counter;
        //      rdma_mg->poll_completion(wc, 1, client_ip, false, compute_node_id);
        if (rdma_mg->try_poll_completions(wc, 1, client_ip, false, compute_node_id) == 0) {
            // exponetial back off to save cpu cycles.
            if (miss_poll_counter < 256) {
                continue;
            }
            if (miss_poll_counter < 512) {
                usleep(16);

                continue;
            }
            if (miss_poll_counter < 1024) {
                usleep(256);

                continue;
            } else {
                usleep(1024);
                continue;
            }
            //        else{
            //
            //          sleep(1);
            //          continue;
            //        }
        }
        miss_poll_counter = 0;
        if (wc[0].wc_flags & IBV_WC_WITH_IMM) {
            wc[0].imm_data;  // use this to find the correct condition variable.
            cv_temp.notify_all();
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, "main");

            // increase the buffer index
            if (buffer_position == R_SIZE - 1) {
                buffer_position = 0;
            } else {
                buffer_position++;
            }
            continue;
        }
        // TOFIX: memory leak for new RDMA request()
        RDMA_Request* receive_msg_buf = new RDMA_Request();
        *receive_msg_buf = *(RDMA_Request*)recv_mr[buffer_position].addr;
        //      memcpy(receive_msg_buf, recv_mr[buffer_position].addr, sizeof(RDMA_Request));

        // copy the pointer of receive buf to a new place because
        // it is the same with send buff pointer.
        // TODO(chuqing): breakpoint here, no use
        if (receive_msg_buf->command == create_mr_) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);

            create_mr_handler(receive_msg_buf, client_ip, compute_node_id);
            //        rdma_mg_->post_send<ibv_mr>(send_mr,client_ip);  // note here should be the mr point to the send
            //        buffer. rdma_mg_->poll_completion(wc, 1, client_ip, true);
        } else if (receive_msg_buf->command == create_qp_) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            create_qp_handler(receive_msg_buf, client_ip, compute_node_id);
            //        rdma_mg_->post_send<registered_qp_config>(send_mr, client_ip);
            //        rdma_mg_->poll_completion(wc, 1, client_ip, true);
        } else if (receive_msg_buf->command == install_version_edit) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            // TODO: implement a durable bg thread and make a shadow verison set if possible.

            install_version_edit_handler(receive_msg_buf, client_ip, compute_node_id);
        } else if (receive_msg_buf->command == near_data_compaction) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            Arg_for_handler* argforhandler = new Arg_for_handler{
                .request = receive_msg_buf, .client_ip = client_ip, .target_node_id = compute_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforhandler};
            Compactor_pool_.Schedule(&Memory_Node_Keeper::RPC_Compaction_Dispatch, thread_pool_args);
            //        sst_compaction_handler(nullptr);
        } else if (receive_msg_buf->command == create_cpu_refresher) {
            // receive a new remote cpu keeper request from compute node
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);

            create_cpu_util_sender(receive_msg_buf, client_ip, compute_node_id);
        } else if (receive_msg_buf->command == request_cpu_utilization) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);

            return_cpu_utilization(receive_msg_buf, client_ip, compute_node_id);
        } else if (receive_msg_buf->command == SSTable_gc) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            Arg_for_handler* argforhandler = new Arg_for_handler{
                .request = receive_msg_buf, .client_ip = client_ip, .target_node_id = compute_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforhandler};
            Message_handler_pool_.Schedule(&Memory_Node_Keeper::RPC_Garbage_Collection_Dispatch, thread_pool_args);
            // TODO: add a handle function for the option value
        } else if (receive_msg_buf->command == flush_helper) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            Arg_for_handler* argforhandler = new Arg_for_handler{
                .request = receive_msg_buf, .client_ip = client_ip, .target_node_id = compute_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforhandler};
            Message_handler_pool_.Schedule(&Memory_Node_Keeper::RPC_Flush_Helper_Dispatch, thread_pool_args);
        } else if (receive_msg_buf->command == hot_to_cold) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            Arg_for_handler* argforhandler = new Arg_for_handler{
                .request = receive_msg_buf, .client_ip = client_ip, .target_node_id = compute_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforhandler};
            Message_handler_pool_.Schedule(&Memory_Node_Keeper::RPC_Hot_To_Cold_Dispatch, thread_pool_args);
        } else if (receive_msg_buf->command == cold_to_hot) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            Arg_for_handler* argforhandler = new Arg_for_handler{
                .request = receive_msg_buf, .client_ip = client_ip, .target_node_id = compute_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforhandler};
            Message_handler_pool_.Schedule(&Memory_Node_Keeper::RPC_Cold_To_Hot_Dispatch, thread_pool_args);
        } else if (receive_msg_buf->command == bucket_scan) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            Arg_for_handler* argforhandler = new Arg_for_handler{
                .request = receive_msg_buf, .client_ip = client_ip, .target_node_id = compute_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforhandler};
            Message_handler_pool_.Schedule(&Memory_Node_Keeper::RPC_Bucket_Scan_Dispatch, thread_pool_args);
        } else if (receive_msg_buf->command == version_unpin_) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            version_unpin_handler(receive_msg_buf, client_ip);
        } else if (receive_msg_buf->command == sync_option) {
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            sync_option_handler(receive_msg_buf, client_ip, compute_node_id);
        } else if (receive_msg_buf->command == qp_reset_) {  // depracated functions
            // THis should not be called because the recevei mr will be reset and the buffer
            //  counter will be reset as 0
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            qp_reset_handler(receive_msg_buf, client_ip, socket_fd, compute_node_id);
            DEBUG("QP has been reconnect from the memory node side\n");
            // TODO: Pause all the background tasks because the remote qp is not ready.
            //  stop sending back messasges. The compute node may not reconnect its qp yet!
        } else if (receive_msg_buf->command == print_cpu_util) {  // depracated functions
            // THis should not be called because the recevei mr will be reset and the buffer
            //  counter will be reset as 0
            rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
            printf("RPC: CPU utilization is %f\n", TimberSaw::Memory_Node_Keeper::rdma_mg->rpter.getCurrentValue());

        } else {
            printf("corrupt message from client. %d\n", receive_msg_buf->command);
            assert(false);
            break;
        }
        // increase the buffer index
        if (buffer_position == R_SIZE - 1) {
            buffer_position = 0;
        } else {
            buffer_position++;
        }
    }
    assert(false);
    // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.
}
void Memory_Node_Keeper::Server_to_Client_Communication() {
    if (rdma_mg->resources_create()) {
        fprintf(stderr, "failed to create resources\n");
    }
    int rc;
    if (rdma_mg->rdma_config.gid_idx >= 0) {
        printf("checkpoint0\n");
        rc = ibv_query_gid(rdma_mg->res->ib_ctx, rdma_mg->rdma_config.ib_port, rdma_mg->rdma_config.gid_idx,
                           &(rdma_mg->res->my_gid));
        if (rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n", rdma_mg->rdma_config.ib_port,
                    rdma_mg->rdma_config.gid_idx);

            return;
        }
    } else
        memset(&(rdma_mg->res->my_gid), 0, sizeof rdma_mg->res->my_gid);
    server_sock_connect(rdma_mg->rdma_config.server_name, rdma_mg->rdma_config.tcp_port);
}
// connection code for server side, will get prepared for multiple connection
// on the same port.
int Memory_Node_Keeper::server_sock_connect(const char* servername, int port) {
    struct addrinfo* resolved_addr = NULL;
    struct addrinfo* iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;
    struct sockaddr address;
    socklen_t len = sizeof(struct sockaddr);
    struct addrinfo hints = {.ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
    if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
    /* Resolve DNS address, use sockfd as temp storage */
    sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
    if (sockfd < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
        goto sock_connect_exit;
    }

    /* Search through results and find the one we want */
    for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
        int option = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int));
        if (sockfd >= 0) {
            /* Server mode. Set up listening socket an accept a connection */
            listenfd = sockfd;
            sockfd = -1;
            if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen)) goto sock_connect_exit;
            listen(listenfd, 20);
            while (1) {
                sockfd = accept(listenfd, &address, &len);
                std::string client_id = std::string(inet_ntoa(((struct sockaddr_in*)(&address))->sin_addr)) +
                                        std::to_string(((struct sockaddr_in*)(&address))->sin_port);
                // Client id must be composed of ip address and port number.
                std::cout << "connection built up from" << client_id << std::endl;
                std::cout << "connection family is " << address.sa_family << std::endl;
                if (sockfd < 0) {
                    fprintf(stderr, "Connection accept error, erron: %d\n", errno);
                    break;
                }
                main_comm_threads.emplace_back(
                    [this](std::string client_ip, int socketfd) {
                        this->server_communication_thread(client_ip, socketfd);
                    },
                    std::string(address.sa_data), sockfd);
                // No need to detach, because the main_comm_threads will not be destroyed.
                //        main_comm_threads.back().detach();
            }
            usleep(1000);
        }
    }
sock_connect_exit:

    if (listenfd) close(listenfd);
    if (resolved_addr) freeaddrinfo(resolved_addr);
    if (sockfd < 0) {
        if (servername)
            fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        else {
            perror("server accept");
            fprintf(stderr, "accept() failed\n");
        }
    }
    return sockfd;
}
void Memory_Node_Keeper::JoinAllThreads(bool wait_for_jobs_to_complete) {
    Compactor_pool_.JoinThreads(wait_for_jobs_to_complete);
}
void Memory_Node_Keeper::create_mr_handler(RDMA_Request* request, std::string& client_ip, uint8_t target_node_id) {
    DEBUG("Create new mr\n");
    //  std::cout << "create memory region command receive for" << client_ip
    //  << std::endl;
    // TODO: consider the edianess of the RDMA request and reply.
    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    *send_pointer = {};

    const size_t requested_size = request->content.create_mr.mem_size;
    const Chunk_type requested_type = request->content.create_mr.c_type;
    ibv_mr* mr = nullptr;
    char* buff;
#if defined(WITHPERSISTENCE) && defined(BOUNDEDMEM)
    {
        std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
        assert(requested_size == 1024 * 1024 * 1024);  // Preallocation requires memory in 1GB units.
        if (!rdma_mg->pre_allocated_pool.empty()) {
            if (!rdma_mg->Local_Memory_Register(&buff, &mr, requested_size, No_Use_Default_chunk)) {
                fprintf(stderr, "memory registering failed by size of 0x%x\n",
                        static_cast<unsigned>(requested_size));
            }
            send_pointer->content.mr = *mr;
        } else {
            rdma_mg->RM_reach_limit = true;
            send_pointer->content.mr.addr = reinterpret_cast<void*>(1);
        }

        //      printf("Now the Remote memory regularated by compute node is %zu GB",
        //             rdma_mg->local_mem_pool.size());
    }

    send_pointer->received = true;
#else
    {
        std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
        assert(requested_size == 1024 * 1024 * 1024);  // Preallocation requires memory in 1GB units.
        if (!rdma_mg->Local_Memory_Register(&buff, &mr, requested_size, No_Use_Default_chunk)) {
            fprintf(stderr, "memory registering failed by size of 0x%x\n",
                    static_cast<unsigned>(requested_size));
        }
        //      printf("Now the Remote memory regularated by compute node is %zu GB",
        //             rdma_mg->local_mem_pool.size());
    }

    send_pointer->content.mr = *mr;
    send_pointer->received = true;
#endif

    if (mr != nullptr && requested_type == ValueZoneChunk) {
        std::lock_guard<std::mutex> lock(value_zone_mr_mu_);
        value_zone_mrs_.push_back(mr);
    }

    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    delete request;
}
// Return the cpu utilization of memory node in the RDMA_Reply
void Memory_Node_Keeper::return_cpu_utilization(RDMA_Request* request, std::string& client_ip, uint8_t target_node_id) {
    DEBUG("Return cpu utilization\n");

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;

    // send_pointer->content.cpu_percent = rdma_mg->rpter.getCurrentValue();
    send_pointer->content.cpu_info.cpu_util = rdma_mg->rpter.current_percent;
    send_pointer->received = true;

    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    delete request;
}

void Memory_Node_Keeper::create_cpu_util_sender(RDMA_Request* request, std::string& client_ip, uint8_t target_node_id) {
    DEBUG("Create cpu utilization sender\n");

    std::thread CPU_utilization_heartbeat([&]() {
        // backup the function arguments
        uint8_t target_node_id_local = target_node_id;
        void* request_buffer = request->buffer;
        // memcpy(request_buffer, request->buffer, sizeof(ibv_mr));
        uint32_t request_rkey = request->rkey;
        std::string client_ip_local = std::string(client_ip.c_str());

        ibv_mr send_mr;
        rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
        RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;

        std::ofstream outfile;
        outfile.open("senderout.txt", std::ios::out);

        send_pointer->received = true;
        while (1) {
            // std::fprintf(stdout, "in cpu sender: %.4lf\n", rdma_mg->rpter.current_percent);
            outfile << "in cpu sender" << rdma_mg->rpter.current_percent << std::endl;
            send_pointer->content.cpu_info.cpu_util = rdma_mg->rpter.current_percent;
            rdma_mg->RDMA_Write(request_buffer, request_rkey, &send_mr, sizeof(RDMA_Reply), client_ip_local,
                                IBV_SEND_SIGNALED, 1, target_node_id_local);
            std::this_thread::sleep_for(std::chrono::milliseconds(CPU_UTILIZATION_CACULATE_INTERVAL));
        }

        outfile.close();
        delete request;
    });
    CPU_utilization_heartbeat.detach();
    // wait for the deepcopy
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
}

void Memory_Node_Keeper::create_cpu_util_heart_beater_sender() {
    DEBUG("Create cpu utilization sender\n");

    std::thread CPU_utilization_heartbeat([&]() {
        // backup the function arguments
        int print_counter = 0;
        // TODO: REmember to recover the while loop and the continue code below.
        while (1) {
            double cpu_util_percentage = rdma_mg->rpter.getCurrentValue();
            if (cpu_util_percentage < 0) {
                continue;
            }
            for (auto iter : rdma_mg->compute_nodes) {
                // register the memory block from the remote memory
                RDMA_Request* send_pointer;
                ibv_mr send_mr = {};
                rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);

                send_pointer = (RDMA_Request*)send_mr.addr;
                send_pointer->command = cpu_utilization_heartbeat;
                send_pointer->content.cpu_info.cpu_util = cpu_util_percentage;
                send_pointer->content.cpu_info.core_number = rdma_mg->rpter.numa_bind_core_num;
                // #ifndef NDEBUG
                if (print_counter++ == 200) {
                    printf("Current cpu utilization is %f\n", cpu_util_percentage);
                    print_counter = 0;
                }
                // #endif

                {
                    std::lock_guard<std::mutex> main_send_lock(rdma_mg->main_send_cq_mutex);
                    rdma_mg->post_send<RDMA_Request>(&send_mr, iter.first, std::string("main"));
                    ibv_wc wc[2] = {};
                    if (rdma_mg->poll_completion(wc, 1, std::string("main"), true, iter.first)) {
                        rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
                        fprintf(stderr, "failed to poll CPU heartbeat send\n");
                        return;
                    }
                }
                rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(CPU_UTILIZATION_CACULATE_INTERVAL));
        }

        //      outfile.close();
        //      delete request;
    });
    CPU_utilization_heartbeat.detach();
    // wait for the deepcopy
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
}

// the client ip can by any string differnt from read_local write_local_flush
// and write_local_compact
void Memory_Node_Keeper::create_qp_handler(RDMA_Request* request, std::string& client_ip, uint8_t target_node_id) {
    int rc;
    DEBUG("Create new qp\n");
    assert(request->buffer != nullptr);
    assert(request->rkey != 0);
    char gid_str[17];
    memset(gid_str, 0, 17);
    memcpy(gid_str, request->content.qp_config.gid, 16);

    // create a unique id for the connection from the compute node
    std::string new_qp_id = std::string(gid_str) + std::to_string(request->content.qp_config.lid) +
                            std::to_string(request->content.qp_config.qp_num);

    //  std::cout << "create query pair command receive for" << client_ip
    //  << std::endl;
    //  fprintf(stdout, "Remote QP number=0x%x\n",
    //          request->content.qp_config.qp_num);
    //  fprintf(stdout, "Remote LID = 0x%x\n",
    //          request->content.qp_config.lid);
    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    ibv_qp* qp = rdma_mg->create_qp_Mside(false, new_qp_id);
    if (rdma_mg->rdma_config.gid_idx >= 0) {
        rc = ibv_query_gid(rdma_mg->res->ib_ctx, rdma_mg->rdma_config.ib_port, rdma_mg->rdma_config.gid_idx,
                           &(rdma_mg->res->my_gid));
        if (rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n", rdma_mg->rdma_config.ib_port,
                    rdma_mg->rdma_config.gid_idx);
            return;
        }
    } else
        memset(&(rdma_mg->res->my_gid), 0, sizeof(rdma_mg->res->my_gid));
    /* exchange using TCP sockets info required to connect QPs */
    send_pointer->content.qp_config.qp_num = rdma_mg->qp_map_Mside[new_qp_id]->qp_num;
    send_pointer->content.qp_config.lid = rdma_mg->res->port_attr.lid;
    memcpy(send_pointer->content.qp_config.gid, &(rdma_mg->res->my_gid), 16);
    send_pointer->received = true;
    registered_qp_config* remote_con_data = new registered_qp_config(request->content.qp_config);
    std::shared_lock<std::shared_mutex> l1(rdma_mg->qp_cq_map_mutex);
    // keep the remote qp information
    rdma_mg->qp_main_connection_info_Mside.insert({new_qp_id, remote_con_data});

    rdma_mg->connect_qp_Mside(qp, new_qp_id);

    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    delete request;
}
void Memory_Node_Keeper::install_version_edit_handler(RDMA_Request* request, std::string& client_ip,
                                                      uint8_t target_node_id) {
    printf("install version\n");
    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    send_pointer->content.ive = {};
    ibv_mr edit_recv_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(edit_recv_mr, Version_edit);
    send_pointer->buffer = edit_recv_mr.addr;
    send_pointer->rkey = edit_recv_mr.rkey;
    assert(request->content.ive.buffer_size < edit_recv_mr.length);
    send_pointer->received = true;
    // TODO: how to check whether the version edit message is ready, we need to know the size of the
    //  version edit in the first REQUEST from compute node.
    volatile char* polling_byte = (char*)edit_recv_mr.addr + request->content.ive.buffer_size - 1;
    memset((void*)polling_byte, 0, 1);

    // #ifndef NDEBUG
    //   memset((char*)edit_recv_mr.addr, 0, 100);
    // #endif

    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);  // IBV_SEND_INLINE

    size_t counter = 0;
    while (*(unsigned char*)polling_byte == 0) {
        _mm_clflush(polling_byte);
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        if (counter == 1000) {
            std::fprintf(stderr, "Polling install version handler\r");
            std::fflush(stderr);
            counter = 0;
        }

        counter++;
    }
    // Will delete the version edit in the background threads.
    VersionEdit* version_edit = new VersionEdit(0);
    version_edit->DecodeFrom(Slice((char*)edit_recv_mr.addr, request->content.ive.buffer_size), 1, table_cache_);
    assert(version_edit->GetNewFilesNum() > 0);
    DEBUG_arg("Version edit decoded, new file number is %zu", version_edit->GetNewFilesNum());
//  std::unique_lock<std::mutex> lck(versionset_mtx, std::defer_lock);
//  versions_->LogAndApply(version_edit, &lck);
// Merge the version edit below.
#ifdef WITHPERSISTENCE
#if CHECKPOINT_TYPE == 1
    {
        std::unique_lock<std::mutex> lck(merger_mtx);
        ve_merger.merge_one_edit(version_edit);
        // NOt digesting enough edit, directly get the next edit.

        // unpin the sstables merged during the edit merge
        if (ve_merger.ready_to_upin_merged_file) {
            // Make sure merged_file_numbers does not contain trival move files!
            UnpinSSTables_RPC(&ve_merger.merged_file_numbers, client_ip, target_node_id);
            ve_merger.ready_to_upin_merged_file = false;
            ve_merger.merged_file_numbers.clear();
        }
#ifndef NDEBUG
        for (auto iter : *ve_merger.GetNewFiles()) {
            printf("The file for this ve_merger is %lu\n", iter.second->number);
        }
#endif
        if (check_point_t_ready.load() == true) {
            VersionEdit_Merger* ve_m = new VersionEdit_Merger();
            ve_m->Swap(&ve_merger);
#ifndef NDEBUG
            for (auto iter : *ve_m->GetNewFiles()) {
                printf("The file for this ve_m is %lu\n", iter.second->number);
            }
#endif
            assert(target_node_id != 0);
            Arg_for_persistent* argforpersistence =
                new Arg_for_persistent{.edit_merger = ve_m, .client_ip = client_ip, .target_node_id = target_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforpersistence};
            assert(Persistency_bg_pool_.queue_len_.load() == 0);

            Persistency_bg_pool_.Schedule(Persistence_Dispatch, thread_pool_args);
            //        ve_merger.Clear();
            check_point_t_ready.store(false);
        }
    }

#else
    {
        VersionEdit_Merger* ve_m = new VersionEdit_Merger();
        ve_m->merge_one_edit(version_edit);
        Arg_for_persistent* argforpersistence =
            new Arg_for_persistent{.edit_merger = ve_m, .client_ip = client_ip, .target_node_id = target_node_id};
        BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforpersistence};
        printf("The bg persistentency queue lenth is %u", Persistency_bg_pool_.queue_len_.load());

        Persistency_bg_pool_.Schedule(Persistence_Dispatch, thread_pool_args);
        //        ve_merger.Clear();
        //    check_point_t_ready.store(false);
    }
#endif
#endif
    //  lck.unlock();
    //  MaybeScheduleCompaction(client_ip);

    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    rdma_mg->Deallocate_Local_RDMA_Slot(edit_recv_mr.addr, Version_edit);
    delete request;
}
void Memory_Node_Keeper::sst_garbage_collection(void* arg) {
    RDMA_Request* request = ((Arg_for_handler*)arg)->request;
    std::string client_ip = ((Arg_for_handler*)arg)->client_ip;
    uint8_t target_node_id = ((Arg_for_handler*)arg)->target_node_id;
    //      printf("Garbage collection\n");

    void* remote_prt = request->buffer;
    uint32_t remote_rkey = request->rkey;
    unsigned int imm_num = request->imm_num;
    Chunk_type c_type = request->content.gc.c_type;
    ibv_mr send_mr;
    //      ibv_mr recv_mr;
    ibv_mr large_recv_mr;
    //      ibv_mr large_send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    //      rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, "message");
    rdma_mg->Allocate_Local_RDMA_Slot(large_recv_mr, Version_edit);
    //      rdma_mg->Allocate_Local_RDMA_Slot(large_send_mr, "version_edit");
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    //    send_pointer->content. = {};
    // set up the communication buffer information.
    //      send_pointer->reply_buffer = recv_mr.addr;
    //      send_pointer->rkey = recv_mr.rkey;
    send_pointer->buffer_large = large_recv_mr.addr;
    send_pointer->rkey_large = large_recv_mr.rkey;

    assert(request->content.gc.buffer_size < large_recv_mr.length);
    send_pointer->received = true;
    // TODO: how to check whether the version edit message is ready, we need to know the size of the
    //  version edit in the first REQUEST from compute node.
    volatile uint64_t* polling_byte =
        (uint64_t*)((char*)large_recv_mr.addr + request->content.gc.buffer_size - sizeof(uint64_t));
    //      memset((char*)large_recv_mr.addr,  0, large_recv_mr.length);
    *polling_byte = 0;
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->RDMA_Write(remote_prt, remote_rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);
    size_t counter = 0;
    // polling the finishing bit for compaction task transmission.
    while (*polling_byte == 0) {
        _mm_clflush(polling_byte);
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        if (counter == 10000) {
            std::fprintf(stderr, "Polling Remote Compaction handler\r");
            std::fflush(stderr);
            counter = 0;
        }

        counter++;
    }
    assert(request->content.gc.buffer_size % sizeof(uint64_t) == 0);
    rdma_mg->BatchGarbageCollection((uint64_t*)large_recv_mr.addr, request->content.gc.buffer_size, c_type);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    //      rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, "message");
    rdma_mg->Deallocate_Local_RDMA_Slot(large_recv_mr.addr, Version_edit);
    //      rdma_mg->Deallocate_Local_RDMA_Slot(large_send_mr.addr, "version_edit");
    delete request;
    delete (Arg_for_handler*)arg;
}

void Memory_Node_Keeper::flush_helper_handler(void* arg) {
    RDMA_Request* request = ((Arg_for_handler*)arg)->request;
    std::string client_ip = ((Arg_for_handler*)arg)->client_ip;
    uint8_t target_node_id = ((Arg_for_handler*)arg)->target_node_id;

    const uint64_t buffer_size = request->content.flush_helper.buffer_size;
    const uint64_t kv_count_in = request->content.flush_helper.kv_count;

    ibv_mr updates_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(updates_mr, FlushBuffer);
    if (buffer_size > updates_mr.length) {
        rdma_mg->Deallocate_Local_RDMA_Slot(updates_mr.addr, FlushBuffer);
        delete request;
        delete (Arg_for_handler*)arg;
        return;
    }

    ibv_mr remote_mr = {};
    remote_mr.addr = request->buffer_large;
    remote_mr.rkey = request->rkey_large;
    if (buffer_size > 0) {
        rdma_mg->RDMA_Read(&remote_mr, &updates_mr, buffer_size, client_ip,
                           IBV_SEND_SIGNALED, 1, target_node_id);
    }

    const size_t pair_size = sizeof(uint64_t) * 4;
    const size_t max_kv_count = buffer_size / pair_size;
    const size_t kv_count = kv_count_in < max_kv_count ? kv_count_in : max_kv_count;

    const char* ptr = static_cast<const char*>(updates_mr.addr);
    size_t error_count = 0;
    const size_t kMaxErrorLogs = 10;
    std::vector<ibv_mr*> value_zone_mrs;
    {
        std::lock_guard<std::mutex> lock(value_zone_mr_mu_);
        value_zone_mrs = value_zone_mrs_;
    }
    std::unique_lock<std::shared_mutex> update_lock(bucket_update_mu_);
    for (size_t i = 0; i < kv_count; ++i) {
        const uint64_t key_index = DecodeFixed64(ptr);
        const uint64_t offset_units = DecodeFixed64(ptr + sizeof(uint64_t));
        const SequenceNumber sequence = DecodeFixed64(ptr + sizeof(uint64_t) * 2);
        const uint64_t bucket_idx = key_index / SLOT_NUM_PER_BUCKET;
        const uint64_t slot_idx = key_index % SLOT_NUM_PER_BUCKET;

        if (bucket_idx >= BUCKET_NUMBER) {
            if (error_count < kMaxErrorLogs) {
                std::fprintf(stderr, "flush_helper: key_index %llu out of bucket range\n",
                             static_cast<unsigned long long>(key_index));
            }
            error_count++;
            ptr += pair_size;
            continue;
        }

        auto builder = GetBucketTableBuilder(bucket_idx);
        if (!builder) {
            if (error_count < kMaxErrorLogs) {
                std::fprintf(stderr, "flush_helper: bucket %llu not registered\n",
                             static_cast<unsigned long long>(bucket_idx));
            }
            error_count++;
            ptr += pair_size;
            continue;
        }

        auto sequence_it = latest_bucket_sequence_by_key_.find(key_index);
        if (sequence_it != latest_bucket_sequence_by_key_.end() && sequence < sequence_it->second) {
            ptr += pair_size;
            continue;
        }

        uint64_t previous_offset_units = 0;
        Status read_status = builder->ReadSlot(slot_idx, &previous_offset_units);
        if (!read_status.ok() ||
            !PatchValueZonePreviousOffset(value_zone_mrs, offset_units, key_index, sequence,
                                          previous_offset_units)) {
            if (error_count < kMaxErrorLogs) {
                std::fprintf(stderr,
                             "flush_helper: failed to link MVCC version key=%llu seq=%llu prev=%llu\n",
                             static_cast<unsigned long long>(key_index),
                             static_cast<unsigned long long>(sequence),
                             static_cast<unsigned long long>(previous_offset_units));
            }
            error_count++;
            ptr += pair_size;
            continue;
        }

        Status s = builder->UpdateSlot(slot_idx, offset_units);
        if (!s.ok()) {
            if (error_count < kMaxErrorLogs) {
                std::fprintf(stderr, "flush_helper: update slot failed: %s\n", s.ToString().c_str());
            }
            error_count++;
        } else {
            latest_bucket_sequence_by_key_[key_index] = sequence;
        }
        ptr += pair_size;
    }

    ibv_mr send_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    *send_pointer = {};
    send_pointer->content.error_count = error_count;
    send_pointer->received = true;
    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply),
                        client_ip, IBV_SEND_SIGNALED, 1, target_node_id);

    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    rdma_mg->Deallocate_Local_RDMA_Slot(updates_mr.addr, FlushBuffer);
    delete request;
    delete (Arg_for_handler*)arg;
}

// Assemble the latest bucket/value-zone records for one key range on the
// memory node and return them in an 8 MB RDMA batch. The cursor allows a
// caller to fetch a large range in a small number of round trips.
void Memory_Node_Keeper::bucket_scan_handler(void* arg) {
    RDMA_Request* request = ((Arg_for_handler*)arg)->request;
    const std::string client_ip = ((Arg_for_handler*)arg)->client_ip;
    const uint8_t target_node_id = ((Arg_for_handler*)arg)->target_node_id;
    const bucket_scan_request scan = request->content.bucket_scan;

    struct ScanItem {
        uint64_t key_index;
        std::string key;
        std::string value;
        SequenceNumber sequence;
        ValueType value_type;
    };
    struct ScanCandidate {
        uint64_t key_index;
        uint64_t head_offset_units;
    };
    std::vector<ScanItem> items;
    std::vector<ScanCandidate> candidates;
    uint64_t error_count = 0;
    std::vector<ibv_mr*> value_zone_mrs;
    {
        std::lock_guard<std::mutex> lock(value_zone_mr_mu_);
        value_zone_mrs = value_zone_mrs_;
    }

    if (scan.start_key < scan.end_key && scan.cursor_key < scan.end_key) {
        // MVCC snapshot: only key discovery and bucket-head capture require
        // metadata synchronization. Version records are immutable and are
        // decoded after releasing this shared lock.
        std::shared_lock<std::shared_mutex> update_lock(bucket_update_mu_);
        for (const auto& key_sequence : latest_bucket_sequence_by_key_) {
            const uint64_t key_index = key_sequence.first;
            if (key_index < std::max(scan.start_key, scan.cursor_key) || key_index >= scan.end_key) {
                continue;
            }
            const uint64_t bucket_idx = key_index / SLOT_NUM_PER_BUCKET;
            const uint64_t slot_idx = key_index % SLOT_NUM_PER_BUCKET;
            auto builder = GetBucketTableBuilder(bucket_idx);
            uint64_t offset_units = 0;
            if (!builder || !builder->ReadSlot(slot_idx, &offset_units).ok() || offset_units == 0) {
                ++error_count;
                continue;
            }
            candidates.push_back({key_index, offset_units});
        }
    }

    for (const ScanCandidate& candidate : candidates) {
        uint64_t offset_units = candidate.head_offset_units;
        bool decoded = false;
        for (size_t depth = 0; offset_units != 0 && depth < 1024; ++depth) {
            std::string key;
            std::string value;
            SequenceNumber sequence = 0;
            ValueType value_type = kTypeValue;
            uint64_t previous_offset_units = 0;
            if (!TryExtractValueFromValueZone(value_zone_mrs, offset_units, candidate.key_index,
                                              &key, &value, &sequence, &value_type,
                                              &previous_offset_units)) {
                ++error_count;
                break;
            }
            decoded = true;
            if (sequence <= scan.max_sequence) {
                items.push_back({candidate.key_index, std::move(key), std::move(value), sequence,
                                 value_type});
                break;
            }
            if (previous_offset_units == offset_units) {
                ++error_count;
                break;
            }
            offset_units = previous_offset_units;
        }
        if (!decoded && candidate.head_offset_units != 0) {
            continue;
        }
    }

    std::sort(items.begin(), items.end(), [](const ScanItem& lhs, const ScanItem& rhs) {
        return lhs.key_index < rhs.key_index;
    });

    ibv_mr output_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(output_mr, FlushBuffer);
    const size_t capacity = std::min<size_t>(scan.buffer_size, output_mr.length);
    std::string output;
    output.reserve(capacity);
    size_t consumed = 0;
    size_t emitted = 0;
    for (; consumed < items.size(); ++consumed) {
        const ScanItem& item = items[consumed];
        const size_t record_size = sizeof(uint64_t) * 5 + item.key.size() + item.value.size();
        if (record_size > capacity) {
            ++error_count;
            if (output.empty()) {
                ++consumed;
            }
            break;
        }
        if (output.size() + record_size > capacity) {
            break;
        }
        PutFixed64(&output, item.key_index);
        PutFixed64(&output, item.sequence);
        PutFixed64(&output, item.value_type == kTypeDeletion ? 1 : 0);
        PutFixed64(&output, item.key.size());
        PutFixed64(&output, item.value.size());
        output.append(item.key);
        output.append(item.value);
        ++emitted;
    }

    if (!output.empty()) {
        std::memcpy(output_mr.addr, output.data(), output.size());
        rdma_mg->RDMA_Write(request->buffer_large, request->rkey_large, &output_mr, output.size(),
                            client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
    }

    ibv_mr send_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* reply = reinterpret_cast<RDMA_Reply*>(send_mr.addr);
    *reply = {};
    reply->content.bucket_scan.buffer_size = output.size();
    reply->content.bucket_scan.entry_count = emitted;
    reply->content.bucket_scan.next_key = consumed < items.size() ? items[consumed].key_index : scan.end_key;
    reply->content.bucket_scan.error_count = error_count;
    reply->content.bucket_scan.done = consumed >= items.size();
    reply->received = true;
    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply),
                        client_ip, IBV_SEND_SIGNALED, 1, target_node_id);

    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    rdma_mg->Deallocate_Local_RDMA_Slot(output_mr.addr, FlushBuffer);
    delete request;
    delete (Arg_for_handler*)arg;
}

// 处理 hot_to_cold 请求：根据 shard_idx 映射单个 bucket 并启动后台线程构建新 SST。
void Memory_Node_Keeper::hot_to_cold_handler(void* arg) {
    RDMA_Request* request = ((Arg_for_handler*)arg)->request;
    std::string client_ip = ((Arg_for_handler*)arg)->client_ip;
    uint8_t target_node_id = ((Arg_for_handler*)arg)->target_node_id;

    const uint64_t shard_idx = request->content.hot_to_cold.shard_idx;
    const uint64_t bucket_idx = shard_idx % BUCKET_NUMBER;
    // Do not acknowledge the transition until the bucket has been materialized
    // as an SST and its slots have been cleared. This keeps reads correct during
    // the cut-over and avoids an untracked detached worker.
    Status transition_status = hot_to_cold_worker(shard_idx, bucket_idx, client_ip, target_node_id);
    if (transition_status.ok()) {
        std::lock_guard<std::mutex> lock(cold_to_hot_meta_mu_);
        bucket_migration_meta_by_shard_.erase(shard_idx);
    }

    ibv_mr send_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    *send_pointer = {};
    send_pointer->content.error_count = transition_status.ok() ? 0 : 1;
    send_pointer->received = true;
    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);

    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    delete request;
    delete (Arg_for_handler*)arg;
}

// 处理 cold_to_hot 请求：仅建立 bucket 元数据，采用 lazy 回迁。
void Memory_Node_Keeper::cold_to_hot_handler(void* arg) {
    RDMA_Request* request = ((Arg_for_handler*)arg)->request;
    std::string client_ip = ((Arg_for_handler*)arg)->client_ip;
    uint8_t target_node_id = ((Arg_for_handler*)arg)->target_node_id;

    const uint64_t shard_idx = request->content.cold_to_hot.shard_idx;
    const uint64_t bucket_idx = shard_idx % BUCKET_NUMBER;
    const uint64_t min_key = request->content.cold_to_hot.min_key;
    const uint64_t max_key = request->content.cold_to_hot.max_key;
    const SequenceNumber cutover_sequence = request->content.cold_to_hot.cutover_sequence;

    ibv_mr bucket_mr = request->content.cold_to_hot.bucket_mr;
    const bool valid_bucket = bucket_mr.addr != nullptr;
    if (valid_bucket) {
        std::lock_guard<std::mutex> lock(bucket_table_mu_);
        bucket_table_mrs_[bucket_idx] = bucket_mr;
        bucket_table_builders_[bucket_idx] =
            std::make_shared<BucketTableBuilderMemorySide>(static_cast<char*>(bucket_mr.addr), bucket_mr.length);
    }

    size_t migrating_subranges = 0;
    size_t bucket_only_subranges = 0;
    if (valid_bucket) {
        BucketMigrationMeta meta;
        meta.shard_idx = shard_idx;
        meta.bucket_idx = bucket_idx;
        meta.min_key = min_key;
        meta.max_key = max_key;
        meta.cutover_sequence = cutover_sequence;
        meta.subrange_overlap_counts.assign(kBucketSubrangeCount, 0);
        meta.subrange_bitmap.assign(kBucketSubrangeCount, 0);
        RecomputeBucketMigrationMeta(&meta);

        for (uint8_t bit : meta.subrange_bitmap) {
            if (bit != 0) {
                ++migrating_subranges;
            }
        }
        bucket_only_subranges = meta.subrange_bitmap.size() - migrating_subranges;

        {
            std::lock_guard<std::mutex> lock(cold_to_hot_meta_mu_);
            bucket_migration_meta_by_shard_[shard_idx] = meta;
        }
        SendBucketMetaUpdateToCompute(meta, client_ip, target_node_id);
    }

    std::fprintf(stdout,
                 "cold_to_hot(meta): shard=%llu, bucket=%llu, key_range=[%llu,%llu), cutover=%llu, "
                 "migrating_subranges=%zu, bucket_only_subranges=%zu\n",
                 static_cast<unsigned long long>(shard_idx), static_cast<unsigned long long>(bucket_idx),
                 static_cast<unsigned long long>(min_key),
                 static_cast<unsigned long long>(max_key), static_cast<unsigned long long>(cutover_sequence),
                 migrating_subranges, bucket_only_subranges);

    ibv_mr send_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    *send_pointer = {};
    send_pointer->content.error_count = valid_bucket ? 0 : 1;
    send_pointer->received = true;
    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);

    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    delete request;
    delete (Arg_for_handler*)arg;
}

// 热转冷后台任务：读取 bucket 中的 KV，并在内存侧生成新的 SSTable。
Status Memory_Node_Keeper::hot_to_cold_worker(uint64_t shard_idx, uint64_t bucket_idx, std::string client_ip,
                                              uint8_t target_node_id) {
    struct KVItem {
        uint64_t key_index;
        uint64_t head_offset_units;
        std::string key;
        std::string value;
        SequenceNumber sequence;
        ValueType value_type;
    };

    std::vector<KVItem> kv_items;
    std::vector<ibv_mr*> value_zone_mrs;
    {
        std::lock_guard<std::mutex> lock(value_zone_mr_mu_);
        value_zone_mrs = value_zone_mrs_;
    }
    auto builder = GetBucketTableBuilder(bucket_idx);
    if (builder) {
        struct SlotSnapshot {
            uint64_t key_index;
            uint64_t offset_units;
        };
        std::vector<SlotSnapshot> slot_snapshot;
        {
            std::shared_lock<std::shared_mutex> update_lock(bucket_update_mu_);
            for (const auto& key_sequence : latest_bucket_sequence_by_key_) {
                const uint64_t key_index = key_sequence.first;
                if (key_index / SLOT_NUM_PER_BUCKET != bucket_idx) {
                    continue;
                }
                const uint64_t slot_idx = key_index % SLOT_NUM_PER_BUCKET;
                uint64_t offset_units = 0;
                Status read_status = builder->ReadSlot(slot_idx, &offset_units);
                if (read_status.ok() && offset_units != 0) {
                    slot_snapshot.push_back({key_index, offset_units});
                }
            }
        }

        for (const SlotSnapshot& snapshot : slot_snapshot) {
            const uint64_t key_index = snapshot.key_index;
            const uint64_t offset_units = snapshot.offset_units;
            std::string key;
            std::string value;
            SequenceNumber sequence = 0;
            ValueType value_type = kTypeValue;
            if (!TryExtractValueFromValueZone(value_zone_mrs, offset_units, key_index,
                                              &key, &value, &sequence, &value_type,
                                              nullptr)) {
                continue;
            }
            kv_items.push_back({key_index, offset_units, std::move(key), std::move(value), sequence,
                                value_type});
        }
    }

    if (kv_items.empty()) {
        std::fprintf(stdout, "hot_to_cold worker: shard=%llu has no kv to build\n",
                     static_cast<unsigned long long>(shard_idx));
        return Status::OK();
    }

    std::sort(kv_items.begin(), kv_items.end(), [](const KVItem& lhs, const KVItem& rhs) {
        return BytewiseComparator()->Compare(Slice(lhs.key), Slice(rhs.key)) < 0;
    });

    auto meta = std::make_shared<RemoteMemTableMetaData>(1);
    meta->shard_target_node_id = rdma_mg->node_id;
    meta->level = 1;
    meta->table_type = block_based;
    {
        std::lock_guard<std::mutex> lock(versionset_mtx);
        meta->number = versions_->NewFileNumber();
    }

    TableBuilder* sst_builder = new TableBuilder_Memoryside(*opts, Flush, rdma_mg);
    bool has_key = false;
    for (const auto& kv : kv_items) {
        std::string internal_key = BuildInternalKey(kv.key, kv.sequence, kv.value_type);
        if (!has_key) {
            meta->smallest.DecodeFrom(Slice(internal_key));
            has_key = true;
        }
        meta->largest.DecodeFrom(Slice(internal_key));
        sst_builder->Add(Slice(internal_key), Slice(kv.value));
    }

    Status finish_status = sst_builder->Finish();
    sst_builder->get_datablocks_map(meta->remote_data_mrs);
    sst_builder->get_dataindexblocks_map(meta->remote_dataindex_mrs);
    sst_builder->get_filter_map(meta->remote_filter_mrs);
    meta->num_entries = sst_builder->get_numentries();
    meta->file_size = sst_builder->FileSize();
    delete sst_builder;

    if (!finish_status.ok()) {
        std::fprintf(stderr, "hot_to_cold worker: build sstable failed, shard=%llu, err=%s\n",
                     static_cast<unsigned long long>(shard_idx), finish_status.ToString().c_str());
        return finish_status;
    }

    {
        std::lock_guard<std::mutex> lock(cold_to_hot_meta_mu_);
        cold_to_hot_pending_ssts_.push_back(meta);
    }

    VersionEdit edit(0);
    edit.AddFile(1, meta);
    {
        std::unique_lock<std::mutex> lck(versionset_mtx);
        Status apply_status = versions_->LogAndApply(&edit);
        if (!apply_status.ok()) {
            std::fprintf(stderr,
                         "hot_to_cold worker: LogAndApply failed, shard=%llu, err=%s\n",
                         static_cast<unsigned long long>(shard_idx), apply_status.ToString().c_str());
            return apply_status;
        }
        Edit_sync_to_remote(&edit, client_ip, &lck, target_node_id);
    }

    size_t cleared_slots = 0;
    if (builder) {
        std::unique_lock<std::shared_mutex> update_lock(bucket_update_mu_);
        for (const KVItem& kv : kv_items) {
            const uint64_t slot_idx = kv.key_index % SLOT_NUM_PER_BUCKET;
            uint64_t current_offset_units = 0;
            if (!builder->ReadSlot(slot_idx, &current_offset_units).ok() ||
                current_offset_units != kv.head_offset_units) {
                continue;
            }
            Status clear_status = builder->UpdateSlot(slot_idx, 0);
            if (clear_status.ok()) {
                auto sequence_it = latest_bucket_sequence_by_key_.find(kv.key_index);
                if (sequence_it != latest_bucket_sequence_by_key_.end() &&
                    sequence_it->second == kv.sequence) {
                    latest_bucket_sequence_by_key_.erase(sequence_it);
                }
                ++cleared_slots;
            }
        }
    }

    std::fprintf(stdout,
                 "hot_to_cold worker: built sstable #%llu at L1, shard=%llu, bucket=%llu, entries=%zu, cleared=%zu\n",
                 static_cast<unsigned long long>(meta->number), static_cast<unsigned long long>(shard_idx),
                 static_cast<unsigned long long>(bucket_idx), meta->num_entries, cleared_slots);
    return Status::OK();
}

void Memory_Node_Keeper::sst_compaction_handler(void* arg) {
    RDMA_Request* request = ((Arg_for_handler*)arg)->request;
    std::string client_ip = ((Arg_for_handler*)arg)->client_ip;
    uint8_t target_node_id = ((Arg_for_handler*)arg)->target_node_id;
    void* remote_prt = request->buffer;
    void* remote_large_prt = request->buffer_large;
    uint32_t remote_rkey = request->rkey;
    uint32_t remote_large_rkey = request->rkey_large;
    unsigned int imm_num = request->imm_num;
    ibv_mr send_mr;
    ibv_mr recv_mr;
    ibv_mr large_recv_mr;
    ibv_mr large_send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, Message);
    rdma_mg->Allocate_Local_RDMA_Slot(large_recv_mr, Version_edit);
    rdma_mg->Allocate_Local_RDMA_Slot(large_send_mr, Version_edit);
    assert(request->content.sstCompact.buffer_size < large_recv_mr.length);
#ifdef WITHPERSISTENCE
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    //    send_pointer->content. = {};
    // set up the communication buffer information.
    send_pointer->buffer = recv_mr.addr;
    send_pointer->rkey = recv_mr.rkey;
    send_pointer->buffer_large = large_recv_mr.addr;
    send_pointer->rkey_large = large_recv_mr.rkey;
    send_pointer->received = true;

    // TODO: how to check whether the version edit message is ready, we need to know the size of the
    //  version edit in the first REQUEST from compute node.
    // DO not need the code below if we use RDMA read to get the compaction.
    //    volatile char* polling_byte = (char*)large_recv_mr.addr + request->content.sstCompact.buffer_size - 1;
    //    memset((void*)polling_byte, 0, 1);
    //    asm volatile ("sfence\n" : : );
    //    asm volatile ("lfence\n" : : );
    //    asm volatile ("mfence\n" : : );
    rdma_mg->RDMA_Write(remote_prt, remote_rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);
#endif

    ibv_mr remote_mr;
    remote_mr.addr = remote_large_prt;
    remote_mr.rkey = remote_large_rkey;
    // NOte we have to use the polling mechanism because other wise the read can be finished ealier
    //  than we want.
    volatile char* polling_byte = (char*)large_recv_mr.addr + request->content.sstCompact.buffer_size - 1;
    memset((void*)polling_byte, 0, 1);
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    // The reason why we have no signal but polling the buffer is that we may
    // poll some completion of other threads RDMA write.
    rdma_mg->RDMA_Read(&remote_mr, &large_recv_mr, request->content.sstCompact.buffer_size + 1, client_ip, 0, 0,
                       target_node_id);
    size_t counter = 0;
    // polling the finishing bit for compaction task transmission.
    while (*(unsigned char*)polling_byte == 0) {
        _mm_clflush(polling_byte);
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        if (counter == 10000) {
            std::fprintf(stderr, "Polling Remote Compaction content\r");
            std::fflush(stderr);
            counter = 0;
        }

        counter++;
    }
    Status status;
    Compaction c(opts.get());
    // Note the RDMA read here could read an unfinished RDMA read.

    // Decode compaction
    c.DecodeFrom(Slice((char*)large_recv_mr.addr, request->content.sstCompact.buffer_size), 1);
    printf("near data compaction at level %d, first level of file%d, second level of file %d\n", c.level(),
           c.num_input_files(0), c.num_input_files(1));

    // Note need to check whether the code below is correct.
    //    void* remote_large_recv_ptr =  *(void**)new_pos.data();
    //    uint32_t remote_large_recv_rkey = *(uint32_t*)(new_pos.data() + sizeof(void*));
    // the slice size is larger than the real size by 1 byte.
    DEBUG_arg("Compaction decoded, the first input file number is %lu \n", c.inputs_[0][0]->number);
    DEBUG_arg("Compaction decoded, input file level is %d \n", c.level());
    CompactionState* compact = new CompactionState(&c);
    if (usesubcompaction && c.num_input_files(0) >= 4 && c.num_input_files(1) > 1) {
        //    if (usesubcompaction && c.num_input_files(1)>1){
        //      test_compaction_mutex.lock();
        status = DoCompactionWorkWithSubcompaction(compact, client_ip);
        //      test_compaction_mutex.unlock();
        //        status = DoCompactionWork(compact, *client_ip);
    } else {
        status = DoCompactionWork(compact, client_ip);
    }
    if (status.ok()) {
        InstallCompactionResultsToComputePreparation(compact);
        UpdateBucketMigrationMetaAfterCompaction(compact->compaction, client_ip, target_node_id);
    } else {
        std::fprintf(stderr, "near-data compaction failed: %s\n", status.ToString().c_str());
        compact->compaction->edit()->Clear();
    }
    // TODO:Send back the new created sstables and wait for another reply.
    std::string serilized_ve;
#ifndef NDEBUG
    auto edit_files_vec = compact->compaction->edit()->GetNewFiles();
    for (auto iter : *edit_files_vec) {
        assert(iter.second->creator_node_id == rdma_mg->node_id);
        assert(iter.second->creator_node_id % 2 == 0);
    }
#endif
    compact->compaction->edit()->EncodeTo(&serilized_ve);
// #ifndef NDEBUG
//     VersionEdit edit;
//     edit.DecodeFrom((char*)serilized_ve.c_str(), 1);
//     assert(edit.GetNewFilesNum() > 0 );
// #endif
//     *(uint32_t*)large_send_mr.addr = serilized_ve.size();
//     memset((char*)large_send_mr.addr, 1, 1);
#ifndef NDEBUG
    memset((char*)large_send_mr.addr, 0, large_send_mr.length);
#endif

    memcpy((char*)large_send_mr.addr, serilized_ve.c_str(), serilized_ve.size());
    memset((char*)large_send_mr.addr + serilized_ve.size(), 1, 1);
    _mm_clflush((char*)large_send_mr.addr + serilized_ve.size());
    assert(serilized_ve.size() + 1 < large_send_mr.length);
    *(size_t*)send_mr.addr = serilized_ve.size() + 1;

    // Prepare the receive buffer for the version edit duribility and the file numbers.
    volatile char* polling_byte_2 = (char*)recv_mr.addr + sizeof(uint64_t);
    memset((void*)polling_byte_2, 0, 1);
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);

    // write back the verison edit size, by the RDMA with immediate
    //    rdma_mg->RDMA_Write_Imme(remote_prt, remote_rkey,
    //                        &send_mr, sizeof(size_t), client_ip,
    //                        IBV_SEND_SIGNALED, 1, imm_num);

    //    rdma_mg->RDMA_Write(remote_prt, remote_rkey,
    //                        &send_mr, sizeof(size_t), client_ip,
    //                        IBV_SEND_SIGNALED, 1);
    _mm_clflush(polling_byte_2);
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->RDMA_Write_Imme(remote_large_prt, remote_large_rkey, &large_send_mr, serilized_ve.size() + 1, client_ip,
                             IBV_SEND_SIGNALED, 1, imm_num, target_node_id);
#ifndef NDEBUG
    debug_counter.fetch_add(1);
#endif
//    rdma_mg->RDMA_Write(remote_large_prt, remote_large_rkey,
//                             &large_send_mr, serilized_ve.size() + 1, client_ip,
//                             IBV_SEND_SIGNALED, 1);
#ifdef WITHPERSISTENCE
    counter = 0;
    // polling the finishing bit for the file number transmission.
    while (*(unsigned char*)polling_byte_2 == 0) {
        _mm_clflush(polling_byte_2);
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        if (counter == 10000) {
            std::fprintf(stderr, "Polling file number return handler\r");
            std::fflush(stderr);
            counter = 0;
        }

        counter++;
    }
    uint64_t file_number_start = *(uint64_t*)recv_mr.addr;
    assert(file_number_start > 0);
    compact->compaction->edit()->SetFileNumbers(file_number_start);
    DEBUG_arg("file number end %lu", file_number_start);
    // The mem copy overhead below is unavoidable. because *compact->compaction will be delted
    VersionEdit* edit = new VersionEdit(0);
    *edit = *compact->compaction->edit();
    {
        std::unique_lock<std::mutex> lck(merger_mtx);
        ve_merger.merge_one_edit(edit);
        // NOt digesting enough edit, directly get the next edit.

        // unpin the sstables merged during the edit merge
        if (ve_merger.ready_to_upin_merged_file) {
            UnpinSSTables_RPC(&ve_merger.merged_file_numbers, client_ip, target_node_id);
            ve_merger.ready_to_upin_merged_file = false;
            ve_merger.merged_file_numbers.clear();
        }
        if (check_point_t_ready.load() == true) {
            VersionEdit_Merger* ve_m = new VersionEdit_Merger(ve_merger);
            Arg_for_persistent* argforpersistence =
                new Arg_for_persistent{.edit_merger = ve_m, .client_ip = client_ip, .target_node_id = target_node_id};
            BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = argforpersistence};
            assert(Persistency_bg_pool_.queue_len_.load() == 0);
            Persistency_bg_pool_.Schedule(Persistence_Dispatch, thread_pool_args);
            ve_merger.Clear();
            check_point_t_ready.store(false);
        }
    }

#endif
    // The validation below should happen before the real edit send back to the compute node
    //  because there is no reference for this test, the sstables can be garbage collected.
    // #ifndef NDEBUG
    //     for(auto pair : *compact->compaction->edit()->GetNewFiles()){
    //       // Verify that the table is usable
    //       Iterator* it = versions_->table_cache_->NewIterator_MemorySide(ReadOptions(), pair.second);
    //       //        s = it->status();
    //
    //       it->SeekToFirst();
    //       while(it->Valid()){
    //         it->Next();
    //       }
    //       printf("Table %p Read successfully after compaction\n", pair.second.get());
    //       delete it;
    //     }
    // #endif
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, Message);
    rdma_mg->Deallocate_Local_RDMA_Slot(large_recv_mr.addr, Version_edit);
    rdma_mg->Deallocate_Local_RDMA_Slot(large_send_mr.addr, Version_edit);

    delete request;
    delete compact;
    delete (Arg_for_handler*)arg;
}
// THis funciton is deprecated now
void Memory_Node_Keeper::qp_reset_handler(RDMA_Request* request, std::string& client_ip, int socket_fd,
                                          uint8_t target_node_id) {
    ibv_mr send_mr;
    char temp_receive[2];
    char temp_send[] = "Q";
    //    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
    //    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    // reset the qp state.
    ibv_qp* qp = rdma_mg->qp_map_Mside.at(client_ip);
    printf("qp number before reset is %d\n", qp->qp_num);

    rdma_mg->modify_qp_to_reset(qp);
    rdma_mg->connect_qp(qp, client_ip, 0);
    printf("qp number after reset is %d\n", qp->qp_num);
    // NOte: This is not correct because we did not recycle the receive mr, so we also
    //   need to repost the receive wr and start from mr #1
    //    send_pointer->received = true;
    //    rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
    //                        &send_mr, sizeof(RDMA_Reply),client_ip, IBV_SEND_SIGNALED,1);
    rdma_mg->sock_sync_data(socket_fd, 1, temp_send, temp_receive);
    delete request;
    //    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
}
void Memory_Node_Keeper::sync_option_handler(RDMA_Request* request, std::string& client_ip, uint8_t target_node_id) {
    DEBUG("SYNC option \n");
    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
    send_pointer->content.ive = {};
    ibv_mr edit_recv_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(edit_recv_mr, Version_edit);
    send_pointer->buffer = edit_recv_mr.addr;
    send_pointer->rkey = edit_recv_mr.rkey;
    assert(request->content.ive.buffer_size < edit_recv_mr.length);
    send_pointer->received = true;
    // TODO: how to check whether the version edit message is ready, we need to know the size of the
    //  version edit in the first REQUEST from compute node.

    // we need buffer_size - 1 to poll the last byte of the buffer.
    volatile char* polling_byte = (char*)edit_recv_mr.addr + request->content.ive.buffer_size - 1;
    memset((void*)polling_byte, 0, 1);
    asm volatile("sfence\n" : :);
    asm volatile("lfence\n" : :);
    asm volatile("mfence\n" : :);
    rdma_mg->RDMA_Write(request->buffer, request->rkey, &send_mr, sizeof(RDMA_Reply), client_ip, IBV_SEND_SIGNALED, 1,
                        target_node_id);

    while (*(unsigned char*)polling_byte == 0) {
        _mm_clflush(polling_byte);
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        std::fprintf(stderr, "Polling sync option handler\n");
        std::fflush(stderr);
    }
    *opts = *static_cast<Options*>(edit_recv_mr.addr);
    opts->ShardInfo = nullptr;
    opts->env = nullptr;
    opts->filter_policy = new InternalFilterPolicy(NewBloomFilterPolicy(opts->bloom_bits));
    opts->comparator = &internal_comparator_;
    Compactor_pool_.SetBackgroundThreads(opts->max_background_compactions);
    printf("Option sync finished\n");
    delete request;
}
void Memory_Node_Keeper::version_unpin_handler(RDMA_Request* request, std::string& client_ip) {
    std::unique_lock<std::mutex> lck(versionset_mtx);
    //    versions_->Unpin_Version_For_Compute(request->content.unpinned_version_id);
    delete request;
}
void Memory_Node_Keeper::Edit_sync_to_remote(VersionEdit* edit, std::string& client_ip,
                                             std::unique_lock<std::mutex>* version_mtx, uint8_t target_node_id) {
    //  std::unique_lock<std::shared_mutex> l(main_qp_mutex);

    //  std::shared_ptr<RDMA_Manager> rdma_mg = env_->rdma_mg;
    // register the memory block from the remote memory
    RDMA_Request* send_pointer;
    ibv_mr send_mr = {};
    ibv_mr send_mr_ve = {};
    ibv_mr receive_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);

    if (edit->IsTrival()) {
        send_pointer = (RDMA_Request*)send_mr.addr;
        send_pointer->command = install_version_edit;
        send_pointer->content.ive.trival = true;
        //    send_pointer->content.ive.buffer_size = serilized_ve.size();
        int level;
        uint64_t file_number;
        uint8_t node_id;
        edit->GetTrivalFile(level, file_number, node_id);
        send_pointer->content.ive.level = level;
        send_pointer->content.ive.file_number = file_number;
        send_pointer->content.ive.node_id = node_id;
        //    send_pointer->content.ive.version_id = versions_->version_id;
        rdma_mg->post_send<RDMA_Request>(&send_mr, target_node_id, client_ip);
        version_mtx->unlock();
        ibv_wc wc[2] = {};
        if (rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id)) {
            fprintf(stderr, "failed to poll send for remote memory register\n");
            return;
        }
    } else {
        rdma_mg->Allocate_Local_RDMA_Slot(send_mr_ve, Version_edit);

        rdma_mg->Allocate_Local_RDMA_Slot(receive_mr, Message);
        std::string serilized_ve;
        edit->EncodeTo(&serilized_ve);
        assert(serilized_ve.size() <= send_mr_ve.length - 1);
        uint8_t check_byte = 0;
        while (check_byte == 0) {
            check_byte = rand() % 256;
        }

        memcpy(send_mr_ve.addr, serilized_ve.c_str(), serilized_ve.size());
        memset((char*)send_mr_ve.addr + serilized_ve.size(), check_byte, 1);

        send_pointer = (RDMA_Request*)send_mr.addr;
        send_pointer->command = install_version_edit;
        send_pointer->content.ive.trival = false;
        send_pointer->content.ive.buffer_size = serilized_ve.size() + 1;
        send_pointer->content.ive.version_id = versions_->version_id;
        send_pointer->content.ive.check_byte = check_byte;
        send_pointer->buffer = receive_mr.addr;
        send_pointer->rkey = receive_mr.rkey;
        RDMA_Reply* receive_pointer;
        receive_pointer = (RDMA_Reply*)receive_mr.addr;
        // Clear the reply buffer for the polling.
        *receive_pointer = {};
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        rdma_mg->post_send<RDMA_Request>(&send_mr, target_node_id, client_ip);
        // TODO: Think of a better way to avoid deadlock and guarantee the same
        //  sequence of verision edit between compute node and memory server.
        version_mtx->unlock();
        ibv_wc wc[2] = {};
        if (rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id)) {
            fprintf(stderr, "failed to poll send for remote memory register\n");
            return;
        }
        printf("Request was sent, sub version id is %lu, polled buffer address is %p, checkbyte is %d\n",
               versions_->version_id, &receive_pointer->received, check_byte);
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        if (!rdma_mg->poll_reply_buffer(receive_pointer))  // poll the receive for 2 entires
        {
            printf("Reply buffer is %p", receive_pointer->buffer);
            printf("Received is %d", receive_pointer->received);
            printf("receive structure size is %lu", sizeof(RDMA_Reply));
            printf("version id is %lu", versions_->version_id);
            exit(0);
        }

        // Note: here multiple threads will RDMA_Write the "main" qp at the same time,
        //  which means the polling result may not belongs to this thread, but it does not
        //  matter in our case because we do not care when will the message arrive at the other side.
        asm volatile("sfence\n" : :);
        asm volatile("lfence\n" : :);
        asm volatile("mfence\n" : :);
        rdma_mg->RDMA_Write(receive_pointer->buffer, receive_pointer->rkey, &send_mr_ve, serilized_ve.size() + 1,
                            client_ip, IBV_SEND_SIGNALED, 1, target_node_id);
        rdma_mg->Deallocate_Local_RDMA_Slot(send_mr_ve.addr, Version_edit);
        rdma_mg->Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
    }
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
}

}  // namespace TimberSaw
