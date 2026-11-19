// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#include "db/db_impl.h"
#include "db/table_cache.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <sys/types.h>
#include <vector>

#include "TimberSaw/cache.h"
#include "TimberSaw/comparator.h"
#include "TimberSaw/db.h"
#include "TimberSaw/env.h"
#include "TimberSaw/filter_policy.h"
#include "TimberSaw/write_batch.h"

#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#define PRINT_BATCH 4000000
// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      fillmixgraph  -- write N values across the key space used by MixGraph
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      seekordered   -- N ordered seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)


const uint64_t MAX_KEY_VALUE = BUCKET_NUMBER * SLOT_NUM_PER_BUCKET - 1;

static uint64_t GetMaxSliceCompatibleKeyValue(size_t key_size) {
    uint64_t range_max = MAX_KEY_VALUE;
    if (key_size == 0) {
        return 0;
    }
    uint64_t digits_max = 0;
    size_t digits = key_size < 10 ? key_size : 10;
    for (size_t i = 0; i < digits; ++i) {
        digits_max = digits_max * 10 + 9;
    }
    if (digits_max < range_max) {
        range_max = digits_max;
    }
    return range_max;
}

static uint64_t MixGraphPhysicalKey(uint64_t ordinal, uint64_t total_keys, size_t key_size) {
    if (total_keys == 0) return 0;
    const uint64_t key_space = GetMaxSliceCompatibleKeyValue(key_size) + 1;
    return static_cast<uint64_t>((static_cast<__uint128_t>(ordinal) * key_space) / total_keys);
}

class MixGraphHotColdLoadMapper {
public:
    MixGraphHotColdLoadMapper(uint64_t total_keys, size_t key_size) {
        counts_.fill(0);
        const uint64_t hot_total = total_keys * 90 / 100;
        counts_[2] = hot_total * 70 / 100;
        counts_[14] = hot_total * 20 / 100;
        counts_[25] = hot_total - counts_[2] - counts_[14];
        uint64_t cold_left = total_keys - hot_total;
        uint64_t cold_index = 0;
        for (size_t range = 0; range < counts_.size(); ++range) {
            if (range == 2 || range == 14 || range == 25) continue;
            counts_[range] = (total_keys - hot_total) / 27 + (cold_left > 0 ? 1 : 0);
            if (cold_left > 0) --cold_left;
            ++cold_index;
        }
        (void)cold_index;
        prefix_[0] = 0;
        for (size_t range = 0; range < counts_.size(); ++range) {
            prefix_[range + 1] = prefix_[range] + counts_[range];
        }
        key_space_ = GetMaxSliceCompatibleKeyValue(key_size) + 1;
    }

    uint64_t PhysicalKey(uint64_t ordinal) const {
        const auto it = std::upper_bound(prefix_.begin(), prefix_.end(), ordinal);
        const size_t range = static_cast<size_t>(it - prefix_.begin() - 1);
        const uint64_t local_ordinal = ordinal - prefix_[range];
        const uint64_t range_start = static_cast<uint64_t>(
            (static_cast<__uint128_t>(range) * key_space_) / counts_.size());
        const uint64_t range_end = static_cast<uint64_t>(
            (static_cast<__uint128_t>(range + 1) * key_space_) / counts_.size());
        const uint64_t range_width = range_end - range_start;
        return range_start + static_cast<uint64_t>(
            (static_cast<__uint128_t>(local_ordinal) * range_width) / counts_[range]);
    }

    const std::array<uint64_t, 30>& counts() const { return counts_; }

private:
    std::array<uint64_t, 30> counts_{};
    std::array<uint64_t, 31> prefix_{};
    uint64_t key_space_ = 0;
};

// Return an unused physical key immediately after a loaded key.  The 50GB
// experiment has far fewer logical keys than physical key slots, so this key
// is guaranteed to lie before the next loaded key and can be used for a true
// insert while retaining exactly the same spatial region as the sampled key.
static uint64_t MixGraphPhysicalInsertKey(uint64_t ordinal, uint64_t variant, uint64_t total_keys, size_t key_size) {
    const uint64_t loaded_key = MixGraphPhysicalKey(ordinal, total_keys, key_size);
    const uint64_t next_loaded_key =
        ordinal + 1 < total_keys ? MixGraphPhysicalKey(ordinal + 1, total_keys, key_size)
                                 : GetMaxSliceCompatibleKeyValue(key_size) + 1;
    return next_loaded_key > loaded_key + variant ? loaded_key + variant : loaded_key;
}

static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,";

// A argument setting the range of key-value pair generated
// Real inserted number per thread = FLAGS_num by default.
static int FLAGS_num = 1000000;

static int FLAGS_loads = -1;

static int FLAGS_load_vs_num = 1;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of consecutive keys requested by each random range query.
static int FLAGS_range_length = 64;

// Per-thread MixGraph operations.  Kept separate from the post-run validation
// read count so a long workload does not turn validation into another workload.
static int FLAGS_mixgraph_ops = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 400;
// Size of each value
static int FLAGS_key_size = 20;
// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// print throughput for every 2 MIllion batch
static bool FLAGS_batchprint = false;

// Count the number of string comparisons performed
static bool FLAGS_comparisons = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Number of bytes to use as a table_cache of uncompressed data.
// Negative means use no table_cache.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

static int FLAGS_block_restart_interval = 1;
// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = 10;

// Common key prefix length.
static int FLAGS_key_prefix = 0;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// if larger than 0, the compute node will always have this number of shards no matter how many memory
// nodes are there. THe shards will be distributed to the memory nodes in a round robin
// manner.
static int FLAGS_fixed_compute_shards_num = 0;
// whether the writer threads aware of the NUMA archetecture.
static bool FLAGS_enable_numa = false;
// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Whether DBImpl may classify hot regions and flush them to bucket/value zone.
static bool FLAGS_enable_hot_cold = true;
static bool FLAGS_hot_cold_load = false;

// Force the current memtable to flush and wait for all flush/compaction work
// after the load and put-only phases. This keeps the following phase from
// measuring background work generated by the previous phase.
static bool FLAGS_wait_between_phases = false;

// Use the db with the following name.
static const char* FLAGS_db = nullptr;

static int FLAGS_readwritepercent = 90;
static int FLAGS_ops_between_duration_checks = 2000;
static int FLAGS_duration = 0;
namespace TimberSaw {

namespace {
TimberSaw::Env* g_env = nullptr;
TimberSaw::RDMA_Manager* rdma_mg = nullptr;
uint64_t number_of_key_total;
uint64_t number_of_key_per_compute;
uint64_t number_of_key_per_shard;
class Duration {
public:
    Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
        max_seconds_ = max_seconds;
        max_ops_ = max_ops;
        ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
        ops_ = 0;
        start_at_ = g_env->NowMicros();
    }

    int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

    bool Done(int64_t increment) {
        if (increment <= 0) increment = 1;  // avoid Done(0) and infinite loops
        ops_ += increment;

        if (max_seconds_) {
            // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
            auto granularity = FLAGS_ops_between_duration_checks;
            if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
                uint64_t now = g_env->NowMicros();
                return ((now - start_at_) / 1000000) >= max_seconds_;
            } else {
                return false;
            }
        } else {
            return ops_ > max_ops_;
        }
    }

private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

// Workload model downloaded from RocksDB's official db_bench MixGraph
// implementation.  These fitted parameters are the ZippyDB/social-graph
// defaults published with that workload.
class MixGraphKeyGenerator {
public:
    struct Sample {
        uint64_t key_id;
        size_t range_index;
        bool is_hot;
        size_t hot_region;
    };

    explicit MixGraphKeyGenerator(int64_t total_keys) {
        constexpr int64_t kKeyRangeNum = 30;
        keyrange_size_ = std::max<int64_t>(1, total_keys / kKeyRangeNum);
    }

    Sample Next(uint64_t random_value, uint64_t total_keys) const {
        constexpr double kKeyA = 0.002312;
        constexpr double kKeyB = 0.3467;
        // 90% of accesses land in three narrow spatial hot regions with a
        // 70/20/10 split inside the hot set.  The remaining 10% are spread
        // over the other 27 regions, leaving an observable cold population.
        // Each region retains the official MixGraph within-range distribution.
        const uint64_t selector = random_value % 10000;
        size_t range_index = 0;
        bool is_hot = selector < 9000;
        size_t hot_region = 3;
        if (selector < 6300) {
            range_index = 2;
            hot_region = 0;
        } else if (selector < 8100) {
            range_index = 14;
            hot_region = 1;
        } else if (selector < 9000) {
            range_index = 25;
            hot_region = 2;
        } else {
            size_t cold_index = static_cast<size_t>((random_value / 10000) % 27);
            range_index = cold_index;
            if (range_index >= 2) ++range_index;
            if (range_index >= 14) ++range_index;
            if (range_index >= 25) ++range_index;
        }
        const double u = static_cast<double>(random_value % keyrange_size_) / keyrange_size_;
        const int64_t seed = static_cast<int64_t>(std::ceil(std::pow(u / kKeyA, 1.0 / kKeyB)));
        Random key_random(static_cast<uint32_t>(seed));
        const uint64_t key_offset = key_random.Next() % keyrange_size_;
        return {(range_index * keyrange_size_ + key_offset) % total_keys, range_index, is_hot, hot_region};
    }

private:
    int64_t keyrange_size_ = 1;
};

static int64_t MixGraphPareto(double u, double theta, double k, double sigma) {
    if (u <= 0.0) u = 1.0 / static_cast<double>(UINT32_MAX);
    const double value = k == 0.0 ? theta - sigma * std::log(u)
                                  : theta + sigma * (std::pow(u, -k) - 1.0) / k;
    return static_cast<int64_t>(std::ceil(value));
}
class CountComparator : public Comparator {
public:
    CountComparator(const Comparator* wrapped) : wrapped_(wrapped) {}
    ~CountComparator() override {}
    int Compare(const Slice& a, const Slice& b) const override {
        count_.fetch_add(1, std::memory_order_relaxed);
        return wrapped_->Compare(a, b);
    }
    const char* Name() const override { return wrapped_->Name(); }
    void FindShortestSeparator(std::string* start, const Slice& limit) const override {
        wrapped_->FindShortestSeparator(start, limit);
    }

    void FindShortSuccessor(std::string* key) const override { return wrapped_->FindShortSuccessor(key); }

    size_t comparisons() const { return count_.load(std::memory_order_relaxed); }

    void reset() { count_.store(0, std::memory_order_relaxed); }

private:
    mutable std::atomic<size_t> count_{0};
    const Comparator* const wrapped_;
};

// Helper for quickly generating random data.
class RandomGenerator {
private:
    std::string data_;
    int pos_;

public:
    RandomGenerator() {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < 1048576) {
            // Add a short fragment that is as compressible as specified
            // by FLAGS_compression_ratio.
            test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
            data_.append(piece);
        }
        pos_ = 0;
    }

    Slice Generate(size_t len) {
        if (pos_ + len > data_.size()) {
            pos_ = 0;
            assert(len < data_.size());
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }
};

class KeyBuffer {
public:
    KeyBuffer() {
        assert(FLAGS_key_prefix < sizeof(buffer_));
        memset(buffer_, '0', FLAGS_key_prefix);
    }
    KeyBuffer& operator=(KeyBuffer& other) = delete;
    KeyBuffer(KeyBuffer& other) = delete;

    void Set(int k) {
        const uint64_t range_max = GetMaxSliceCompatibleKeyValue(20);
        const uint64_t normalized = static_cast<uint64_t>(k) % (range_max + 1);
        std::snprintf(buffer_ + FLAGS_key_prefix, sizeof(buffer_) - FLAGS_key_prefix, "%020d",
                      static_cast<int>(normalized));  //%016d means preceeding with 0s
    }

    Slice slice() const { return Slice(buffer_, FLAGS_key_prefix + 20); }

private:
    char buffer_[1024];
};

#if defined(__linux)
static Slice TrimSpace(Slice s) {
    size_t start = 0;
    while (start < s.size() && isspace(s[start])) {
        start++;
    }
    size_t limit = s.size();
    while (limit > start && isspace(s[limit - 1])) {
        limit--;
    }
    return Slice(s.data() + start, limit - start);
}
#endif

static void AppendWithSpace(std::string* str, Slice msg) {
    if (msg.empty()) return;
    if (!str->empty()) {
        str->push_back(' ');
    }
    str->append(msg.data(), msg.size());
}

class Stats {
private:
    double start_;
    double finish_;
    double seconds_;
    int done_;
    int next_report_;
    int64_t bytes_;
    int64_t batch_bytes_;

    double last_op_finish_;
    Histogram hist_;
    std::string message_;

public:
    std::atomic<unsigned int> batch_done_;
    std::atomic<unsigned int> batch_found;
    std::atomic<double> last_batch_finish;

    Stats() { Start(); }

    void Start() {
        next_report_ = 100;
        hist_.Clear();
        done_ = 0;
        bytes_ = 0;
        seconds_ = 0;
        message_.clear();
        start_ = finish_ = last_op_finish_ = last_batch_finish = g_env->NowMicros();
    }

    void Merge(const Stats& other) {
        hist_.Merge(other.hist_);
        done_ += other.done_;
        bytes_ += other.bytes_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty()) message_ = other.message_;
    }
    void Merge_batch(Stats& other) {
        // Merge should be thread safe and clear the batch every call, we need to pass another
        // argument for a central stat.
        batch_done_ += other.batch_done_.load();
        other.batch_done_.store(0);
        batch_found += other.batch_found.load();
        other.batch_found.store(0);
        //    done_ += other.done_;
        //    bytes_ += other.bytes_;
        //    seconds_ += other.seconds_;
        //    if (other.start_ < start_) start_ = other.start_;
        //    if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        //    if (message_.empty()) message_ = other.message_;
    }

    void Stop() {
        finish_ = g_env->NowMicros();
        seconds_ = (finish_ - start_) * 1e-6;
    }

    void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

    void FinishedSingleOp() {
        if (FLAGS_histogram) {
            double now = g_env->NowMicros();
            double micros = now - last_op_finish_;
            hist_.Add(micros);
            if (micros > 20000) {
                std::fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
                std::fflush(stderr);
            }
            last_op_finish_ = now;
        }

        done_++;
        batch_done_.fetch_add(1);
        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            std::fprintf(stderr, "... finished %d ops%30s\r", done_, "");
            std::fflush(stderr);

            //      if (FLAGS_batchprint && done_%2000000 == 0){
            //        // print the throughput since last time
            //        double now = g_env->NowMicros();
            //        double elapsed = (now - last_batch_finish) * 1e-6;
            //        std::string extra;
            //        if (bytes_ > 0) {
            //          // Rate is computed on actual elapsed time, not the sum of per-thread
            //          // elapsed times.
            //
            //          char rate[100];
            //          std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
            //                        (bytes_ / 1048576.0) / elapsed);
            //          extra = rate;
            //        }
            //        AppendWithSpace(&extra, message_);
            //
            //        std::fprintf(stdout, "%-12s: %8.3f micros/op; %ld ops/sec;%s%s\n",
            //                     name.ToString().c_str(), seconds_ * 1e6 / done_, (long)(done_/elapsed),
            //                     (extra.empty() ? "" : " "), extra.c_str());
            //      }
        }
    }
    void FinishedMultipleOp(int num) {
        if (FLAGS_histogram) {
            double now = g_env->NowMicros();
            double micros = now - last_op_finish_;
            hist_.Add(micros);
            if (micros > 20000) {
                std::fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
                std::fflush(stderr);
            }
            last_op_finish_ = now;
        }

        done_ = done_ + num;
        batch_done_.fetch_add(1);

        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            std::fprintf(stderr, "... finished %d ops%30s\r", done_, "");
            std::fflush(stderr);
        }
    }

    void AddBytes(int64_t n) { bytes_ += n; }

    void Report(const Slice& name) {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (done_ < 1) done_ = 1;
        double elapsed = (finish_ - start_) * 1e-6;
        std::string extra;
        if (bytes_ > 0) {
            // Rate is computed on actual elapsed time, not the sum of per-thread
            // elapsed times.

            char rate[100];
            std::snprintf(rate, sizeof(rate), "%6.1f MB/s", (bytes_ / 1048576.0) / elapsed);
            extra = rate;
        }
        AppendWithSpace(&extra, message_);

        std::fprintf(stdout, "%-12s: %8.3f micros/op; %ld ops/sec;%s%s\n", name.ToString().c_str(),
                     seconds_ * 1e6 / done_, (long)(done_ / elapsed), (extra.empty() ? "" : " "), extra.c_str());

        if (FLAGS_histogram) {
            std::fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
        }
        std::fflush(stdout);
    }
    void Report_Batch(const Slice& name, int thread_num, FILE* file) {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (batch_done_ < 1) batch_done_ = 1;
        double now = g_env->NowMicros();
        double elapsed = (now - last_batch_finish.load()) * 1e-6;
        last_batch_finish.store(now);
        //    std::string extra;
        //    if (bytes_ > 0) {
        //      // Rate is computed on actual elapsed time, not the sum of per-thread
        //      // elapsed times.
        //
        //      char rate[100];
        //      std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
        //                    (bytes_ / 1048576.0) / elapsed);
        //      extra = rate;
        //    }
        //    AppendWithSpace(&extra, message_);

        std::fprintf(file,
                     "Batch throughput %-12s: %8.3f micros/op; %ld ops/sec;, time stamp is %f, (%d of %d found)\n",
                     name.ToString().c_str(), elapsed * (thread_num - 1) * 1e6 / batch_done_,
                     (long)(batch_done_ / elapsed), now - start_, batch_found.load(), batch_done_.load());

        std::fflush(file);
        batch_done_.store(0);
        batch_found.store(0);
    }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
    port::Mutex mu;
    port::CondVar cv GUARDED_BY(mu);
    port::Mutex mu_1;
    port::CondVar cv_1 GUARDED_BY(mu_1);
    bool batch_ready = false;
    int total GUARDED_BY(mu);

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    int num_initialized GUARDED_BY(mu);
    std::atomic<int> num_done;
    bool start GUARDED_BY(mu);

    SharedState(int total) : cv(&mu), cv_1(&mu_1), total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;        // 0..n-1 when running in n threads
    Random64 rand;  // Has different seeds for different threads
                    //  Random rand;
    Stats stats;
    SharedState* shared;

    ThreadState(int index, int seed) : tid(index), rand(seed), shared(nullptr) {}
};

}  // namespace

class Benchmark {
private:
    Cache* cache_;
    const FilterPolicy* filter_policy_;
    DB* db_;
    int num_;
    int value_size_;
    int entries_per_batch_;
    WriteOptions write_options_;
    int reads_;
    int heap_counter_;
    CountComparator count_comparator_;
    int total_thread_count_;
    std::vector<std::string> validation_keys;
    std::atomic<uint64_t> range_scan_queries_{0};
    std::atomic<uint64_t> range_scan_entries_{0};
    std::atomic<uint64_t> range_scan_bucket_errors_{0};
    std::atomic<uint64_t> mixgraph_get_ops_{0};
    std::atomic<uint64_t> mixgraph_get_found_ops_{0};
    std::atomic<uint64_t> mixgraph_get_not_found_ops_{0};
    std::atomic<uint64_t> mixgraph_get_load_ops_{0};
    std::atomic<uint64_t> mixgraph_get_load_not_found_ops_{0};
    std::atomic<uint64_t> mixgraph_get_insert_ops_{0};
    std::atomic<uint64_t> mixgraph_get_insert_not_found_ops_{0};
    std::atomic<uint64_t> mixgraph_put_ops_{0};
    std::atomic<uint64_t> mixgraph_get_nanos_{0};
    std::atomic<uint64_t> mixgraph_put_nanos_{0};
    std::atomic<uint64_t> mixgraph_get_hot_ops_{0};
    std::atomic<uint64_t> mixgraph_get_cold_ops_{0};
    std::atomic<uint64_t> mixgraph_put_hot_ops_{0};
    std::atomic<uint64_t> mixgraph_put_cold_ops_{0};
    std::atomic<uint64_t> mixgraph_put_update_ops_{0};
    std::atomic<uint64_t> mixgraph_put_insert_ops_{0};
    std::array<std::atomic<uint64_t>, 3> mixgraph_get_hot_region_ops_{};
    std::array<std::atomic<uint64_t>, 3> mixgraph_put_hot_region_ops_{};
    std::array<std::atomic<uint64_t>, 30> mixgraph_insert_sequences_{};
    std::mutex mixgraph_latency_mutex_;
    std::vector<uint64_t> mixgraph_get_latency_nanos_;
    std::vector<uint64_t> mixgraph_put_latency_nanos_;
    struct InsertedKeyRecord {
        uint64_t physical_key;
        bool is_hot;
        size_t hot_region;
    };
    std::vector<InsertedKeyRecord> mixgraph_inserted_keys_;

    static double LatencyPercentileMicros(const std::vector<uint64_t>& sorted_nanos, double percentile) {
        if (sorted_nanos.empty()) return 0.0;
        const double rank = (percentile / 100.0) * static_cast<double>(sorted_nanos.size() - 1);
        const size_t lower = static_cast<size_t>(rank);
        const size_t upper = std::min(lower + 1, sorted_nanos.size() - 1);
        const double fraction = rank - static_cast<double>(lower);
        const double nanos = sorted_nanos[lower] + (sorted_nanos[upper] - sorted_nanos[lower]) * fraction;
        return nanos / 1000.0;
    }

    struct InsertKeySlot {
        uint64_t ordinal;
        uint64_t variant;
    };

    InsertKeySlot NextInsertKeySlot(size_t range_index, uint64_t total_keys) {
        constexpr uint64_t kKeyRangeNum = 30;
        const uint64_t range_size = std::max<uint64_t>(1, total_keys / kKeyRangeNum);
        const uint64_t offset = mixgraph_insert_sequences_[range_index].fetch_add(1, std::memory_order_relaxed);
        return {(range_index * range_size + offset % range_size) % total_keys, offset / range_size + 1};
    }

    void PrintHeader() {
        const int kKeySize = 20 + FLAGS_key_prefix;
        PrintEnvironment();
        std::fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
        std::fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n", FLAGS_value_size,
                     static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
        std::fprintf(stdout, "Entries:    %d\n", num_);
        std::fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                     ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_) / 1048576.0));
        std::fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
                     (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_) / 1048576.0));
        PrintWarnings();
        std::fprintf(stdout, "------------------------------------------------\n");
    }

    void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
        std::fprintf(stdout, "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
        std::fprintf(stdout, "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

        // See if snappy is working by attempting to compress a compressible string
        const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
        std::string compressed;
        if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
            std::fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
        } else if (compressed.size() >= sizeof(text)) {
            std::fprintf(stdout, "WARNING: Snappy compression is not effective\n");
        }
    }

    void PrintEnvironment() {
        std::fprintf(stderr, "TimberSaw:    version %d.%d\n", kMajorVersion, kMinorVersion);

#if defined(__linux)
        time_t now = time(nullptr);
        std::fprintf(stderr, "Date:       %s",
                     ctime(&now));  // ctime() adds newline

        FILE* cpuinfo = std::fopen("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
                const char* sep = strchr(line, ':');
                if (sep == nullptr) {
                    continue;
                }
                Slice key = TrimSpace(Slice(line, sep - 1 - line));
                Slice val = TrimSpace(Slice(sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val.ToString();
                } else if (key == "table_cache size") {
                    cache_size = val.ToString();
                }
            }
            std::fclose(cpuinfo);
            std::fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
            std::fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
        }
#endif
    }

public:
    Benchmark()
        : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : nullptr),
          filter_policy_(FLAGS_bloom_bits >= 0 ? NewBloomFilterPolicy(FLAGS_bloom_bits) : nullptr),
          db_(nullptr),
          num_(FLAGS_num),
          value_size_(FLAGS_value_size),
          entries_per_batch_(1),
          reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
          heap_counter_(0),
          count_comparator_(BytewiseComparator()),
          total_thread_count_(0) {
        std::vector<std::string> files;
        g_env->GetChildren(FLAGS_db, &files);
        for (size_t i = 0; i < files.size(); i++) {
            if (Slice(files[i]).starts_with("heap-")) {
                g_env->RemoveFile(std::string(FLAGS_db) + "/" + files[i]);
            }
        }
        if (!FLAGS_use_existing_db) {
            DestroyDB(FLAGS_db, Options());
        }
    }

    ~Benchmark() {
        delete db_;
        delete cache_;
        delete filter_policy_;
    }
    Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
        char* data = new char[FLAGS_key_size + 1];
        memset(data, '0', FLAGS_key_size);
        data[FLAGS_key_size] = '\0';
        const char* const_data = data;
        key_guard->reset(const_data);
        return Slice(key_guard->get(), FLAGS_key_size);
    }
    Slice AllocateKey(std::unique_ptr<const char[]>* key_guard, size_t key_size) {
        char* data = new char[key_size + 1];
        memset(data, '0', key_size);
        data[key_size] = '\0';
        const char* const_data = data;
        key_guard->reset(const_data);
        return Slice(key_guard->get(), key_size);
    }
    void GenerateKeyFromInt(uint64_t v, Slice* key) {
        char* start = const_cast<char*>(key->data());
        size_t key_size = key->size();
        if (key_size == 0) {
            return;
        }
        const uint64_t range_max = GetMaxSliceCompatibleKeyValue(key_size);
        const uint64_t normalized = v % (range_max + 1);
        std::snprintf(start, key_size + 1, "%0*llu", static_cast<int>(key_size),
                      static_cast<unsigned long long>(normalized));
    }
    void Run() {
        PrintHeader();
        Open();

        const char* benchmarks = FLAGS_benchmarks;
        //    Validation_Write();
        while (benchmarks != nullptr) {
            const char* sep = strchr(benchmarks, ',');
            Slice name;
            if (sep == nullptr) {
                name = benchmarks;
                benchmarks = nullptr;
            } else {
                name = Slice(benchmarks, sep - benchmarks);
                benchmarks = sep + 1;
            }

            // Reset parameters that may be overridden below
            num_ = FLAGS_num;
            reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
            value_size_ = FLAGS_value_size;
            entries_per_batch_ = 1;
            write_options_ = WriteOptions();

            void (Benchmark::*method)(ThreadState*) = nullptr;
            bool fresh_db = false;
            int num_threads = FLAGS_threads;

            if (name == Slice("open")) {
                method = &Benchmark::OpenBench;
                num_ /= 10000;
                if (num_ < 1) num_ = 1;
            } else if (name == Slice("fillseq")) {
                fresh_db = true;
                method = &Benchmark::WriteSeq;
            } else if (name == Slice("fillbatch")) {
                fresh_db = true;
                entries_per_batch_ = 1000;
                method = &Benchmark::WriteSeq;
            } else if (name == Slice("fillrandom")) {
                fresh_db = true;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("fillmixgraph")) {
                fresh_db = true;
                method = &Benchmark::WriteMixGraphKeys;
            } else if (name == Slice("fillrandomshard")) {
                fresh_db = true;
                method = &Benchmark::WriteRandomSharded;
            } else if (name == Slice("overwrite")) {
                fresh_db = false;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("fillsync")) {
                fresh_db = true;
                num_ /= 1000;
                write_options_.sync = true;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("fill100K")) {
                fresh_db = true;
                num_ /= 1000;
                value_size_ = 100 * 1000;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("readseq")) {
                method = &Benchmark::ReadSequential;
            } else if (name == Slice("readreverse")) {
                method = &Benchmark::ReadReverse;
            } else if (name == Slice("readrandom")) {
                method = &Benchmark::ReadRandom;
            } else if (name == Slice("readrandomrange")) {
                method = &Benchmark::RangeReadRandom;
            } else if (name == Slice("readrandomshard")) {
                method = &Benchmark::ReadRandom_Sharded;
            } else if (name == Slice("readrandomwriterandom")) {
                method = &Benchmark::ReadRandomWriteRandom;
            } else if (name == Slice("mixgraph")) {
                method = &Benchmark::MixGraph;
            } else if (name == Slice("putmixgraph")) {
                method = &Benchmark::PutMixGraph;
            } else if (name == Slice("getmixgraph")) {
                method = &Benchmark::GetMixGraph;
            } else if (name == Slice("readmixgraph")) {
                method = &Benchmark::ReadMixGraphKeys;
            } else if (name == Slice("readmissing")) {
                method = &Benchmark::ReadMissing;
            } else if (name == Slice("seekrandom")) {
                method = &Benchmark::SeekRandom;
            } else if (name == Slice("seekordered")) {
                method = &Benchmark::SeekOrdered;
            } else if (name == Slice("readhot")) {
                method = &Benchmark::ReadHot;
            } else if (name == Slice("readrandomsmall")) {
                reads_ /= 1000;
                method = &Benchmark::ReadRandom;
            } else if (name == Slice("deleteseq")) {
                method = &Benchmark::DeleteSeq;
            } else if (name == Slice("deleterandom")) {
                method = &Benchmark::DeleteRandom;
            } else if (name == Slice("readwhilewriting")) {
                num_threads++;  // Add extra thread for writing
                method = &Benchmark::ReadWhileWriting;
            } else if (name == Slice("readwhilewriting_fixednum")) {
                num_threads++;  // Add extra thread for writing
                method = &Benchmark::ReadWhileWriting_fixnum;
            } else if (name == Slice("compact")) {
                method = &Benchmark::Compact;
            } else if (name == Slice("crc32c")) {
                method = &Benchmark::Crc32c;
            } else if (name == Slice("snappycomp")) {
                method = &Benchmark::SnappyCompress;
            } else if (name == Slice("snappyuncomp")) {
                method = &Benchmark::SnappyUncompress;
            } else if (name == Slice("heapprofile")) {
                HeapProfile();
            } else if (name == Slice("stats")) {
                PrintStats("TimberSaw.stats");
            } else if (name == Slice("sstables")) {
                PrintStats("TimberSaw.sstables");
            } else {
                if (!name.empty()) {  // No error message for empty name
                    std::fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
                }
            }

            if (fresh_db) {
                if (FLAGS_use_existing_db) {
                    std::fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n", name.ToString().c_str());
                    method = nullptr;
                } else {
                    delete db_;
                    db_ = nullptr;
                    DestroyDB(FLAGS_db, Options());
                    Open();
                    DEBUG("The second open finished.\n");
                }
            }

            if (method != nullptr) {
#ifdef PROCESSANALYSIS
                if (method == &Benchmark::ReadRandom || method == &Benchmark::ReadWhileWriting) {
                    TableCache::CleanAll();
                }
#endif
                DEBUG("The benchmark start.\n");
                if (method == &Benchmark::PutMixGraph) {
                    std::printf("BENCH_PHASE name=putmixgraph state=start ts_us=%llu\n",
                                static_cast<unsigned long long>(g_env->NowMicros()));
                    std::fflush(stdout);
                }
                RunBenchmark(num_threads, name, method);
                DEBUG("Benchmark finished\n");
                if (FLAGS_wait_between_phases &&
                    (method == &Benchmark::WriteMixGraphKeys || method == &Benchmark::PutMixGraph ||
                     method == &Benchmark::MixGraph)) {
                    const uint64_t wait_start_us = g_env->NowMicros();
                    std::printf("background_wait_after_%s: begin\n", name.ToString().c_str());
                    std::fflush(stdout);
                    db_->WaitforAllbgtasks(true);
                    const uint64_t wait_elapsed_us = g_env->NowMicros() - wait_start_us;
                    std::printf("background_wait_after_%s: end elapsed_us=%llu\n", name.ToString().c_str(),
                                static_cast<unsigned long long>(wait_elapsed_us));
                    if (method == &Benchmark::PutMixGraph) {
                        std::printf("BENCH_PHASE name=putmixgraph state=end ts_us=%llu\n",
                                    static_cast<unsigned long long>(g_env->NowMicros()));
                    }
                    std::fflush(stdout);
                }
            }
        }
        //    Validation_Read();
    }

private:
    struct ThreadArg {
        Benchmark* bm;
        SharedState* shared;
        ThreadState* thread;
        void (Benchmark::*method)(ThreadState*);
    };

    static void ThreadBody(void* v) {
        ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
        SharedState* shared = arg->shared;
        ThreadState* thread = arg->thread;
        printf("Wait for thread start\n");
        {
            MutexLock l(&shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.SignalAll();
            }
            while (!shared->start) {
                shared->cv.Wait();
            }
        }
        printf("Threads start to run\n");
        thread->stats.Start();
        (arg->bm->*(arg->method))(thread);
        thread->stats.Stop();

        {
            MutexLock l(&shared->mu);
            shared->num_done.fetch_add(1);
            if (shared->num_done >= shared->total) {
                shared->cv.SignalAll();
            }
        }
    }
    void print_throuput_every_batch(ThreadArg* arg, int n, Stats* batch_print_stat, SharedState* shared_state,
                                    int thread_num, Slice name) {
        batch_print_stat->Start();
        FILE* file = fopen("throughput_batch_print.txt", "a");
        while (shared_state->num_done != thread_num) {
            shared_state->mu_1.Lock();
            while (!shared_state->batch_ready) {
                shared_state->cv_1.Wait();
            }
            shared_state->batch_ready = false;

            for (int i = 0; i < n; i++) {
                batch_print_stat->Merge_batch(arg[i].thread->stats);
            }
            batch_print_stat->Report_Batch(name, n, file);
            shared_state->mu_1.Unlock();
        }
        fclose(file);
    }

    void RunBenchmark(int n, Slice name, void (Benchmark::*method)(ThreadState*)) {
        //    printf("Bechmark start\n");
        if (method == &Benchmark::WriteRandom || method == &Benchmark::WriteRandomSharded) Validation_Write();
        if (method == &Benchmark::WriteMixGraphKeys && FLAGS_hot_cold_load) {
            MixGraphHotColdLoadMapper mapper(static_cast<uint64_t>(FLAGS_num) * FLAGS_threads,
                                             static_cast<size_t>(FLAGS_key_size));
            const auto& counts = mapper.counts();
            std::printf("fillmixgraph_hot_cold_load: entries=%llu hot=%llu cold=%llu "
                        "regions_2_14_25=%llu/%llu/%llu\n",
                        static_cast<unsigned long long>(FLAGS_num) * FLAGS_threads,
                        static_cast<unsigned long long>(counts[2] + counts[14] + counts[25]),
                        static_cast<unsigned long long>(FLAGS_num * FLAGS_threads - counts[2] - counts[14] - counts[25]),
                        static_cast<unsigned long long>(counts[2]),
                        static_cast<unsigned long long>(counts[14]),
                        static_cast<unsigned long long>(counts[25]));
            std::fflush(stdout);
        }
        const bool is_mixgraph_phase = method == &Benchmark::MixGraph || method == &Benchmark::PutMixGraph ||
                                       method == &Benchmark::GetMixGraph;
        if (method == &Benchmark::RangeReadRandom) {
            range_scan_queries_.store(0, std::memory_order_relaxed);
            range_scan_entries_.store(0, std::memory_order_relaxed);
            range_scan_bucket_errors_.store(0, std::memory_order_relaxed);
        }
        if (is_mixgraph_phase) {
            mixgraph_get_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_found_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_not_found_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_load_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_load_not_found_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_insert_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_insert_not_found_ops_.store(0, std::memory_order_relaxed);
            mixgraph_put_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_nanos_.store(0, std::memory_order_relaxed);
            mixgraph_put_nanos_.store(0, std::memory_order_relaxed);
            mixgraph_get_hot_ops_.store(0, std::memory_order_relaxed);
            mixgraph_get_cold_ops_.store(0, std::memory_order_relaxed);
            mixgraph_put_hot_ops_.store(0, std::memory_order_relaxed);
            mixgraph_put_cold_ops_.store(0, std::memory_order_relaxed);
            mixgraph_put_update_ops_.store(0, std::memory_order_relaxed);
            mixgraph_put_insert_ops_.store(0, std::memory_order_relaxed);
            for (size_t i = 0; i < mixgraph_get_hot_region_ops_.size(); ++i) {
                mixgraph_get_hot_region_ops_[i].store(0, std::memory_order_relaxed);
                mixgraph_put_hot_region_ops_[i].store(0, std::memory_order_relaxed);
            }
            for (auto& sequence : mixgraph_insert_sequences_) {
                sequence.store(0, std::memory_order_relaxed);
            }
            std::lock_guard<std::mutex> lock(mixgraph_latency_mutex_);
            mixgraph_get_latency_nanos_.clear();
            mixgraph_put_latency_nanos_.clear();
            if (method == &Benchmark::MixGraph || method == &Benchmark::PutMixGraph) {
                mixgraph_inserted_keys_.clear();
            }
        }
        //    if (name.ToString() == "readrandom"){
        //    }
        SharedState shared(n);
        // TODO(chuqing): try to activate here, but failed
        ThreadArg* arg = new ThreadArg[n];
        for (int i = 0; i < n; i++) {
#ifdef NUMA
            if (FLAGS_enable_numa) {
                // Performs a local allocation of memory to threads in numa node.
                int n_nodes = numa_num_task_nodes();  // Number of nodes in NUMA.
                numa_exit_on_error = 1;
                int numa_node = i % n_nodes;
                bitmask* nodes = numa_allocate_nodemask();
                numa_bitmask_clearall(nodes);
                numa_bitmask_setbit(nodes, numa_node);
                // numa_bind() call binds the process to the node and these
                // properties are passed on to the thread that is created in
                // StartThread method called later in the loop.
                numa_bind(nodes);
                numa_set_strict(1);
                numa_free_nodemask(nodes);
            }
#endif
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            ++total_thread_count_;
            // Seed the thread's random state deterministically based upon thread
            // creation across all benchmarks. This ensures that the seeds are unique
            // but reproducible when rerunning the same set of benchmarks.
            arg[i].thread = new ThreadState(i, /*seed=*/1000 + total_thread_count_);
            arg[i].thread->shared = &shared;
            printf("start front-end threads\n");
            g_env->StartThread(ThreadBody, &arg[i]);
        }
        std::thread* t_print = nullptr;
        if (FLAGS_batchprint) {
            Stats batch_stat;
            t_print =
                new std::thread(&Benchmark::print_throuput_every_batch, this, arg, n, &batch_stat, &shared, n, name);
            t_print->detach();
        }

        shared.mu.Lock();
        while (shared.num_initialized < n) {
            shared.cv.Wait();
        }
        // sync with all the other compute nodes here
        rdma_mg->sync_with_computes_Cside();

        const uint64_t benchmark_start_us = g_env->NowMicros();
        shared.start = true;
        rdma_mg->Print_Remote_CPU_RPC(0);
        shared.cv.SignalAll();
        while (shared.num_done < n) {
            shared.cv.Wait();
        }
        shared.mu.Unlock();
        const uint64_t benchmark_elapsed_us = g_env->NowMicros() - benchmark_start_us;
        //    if (FLAGS_batchprint){
        //      t_print->join();
        //      delete t_print;
        //    }
        for (int i = 1; i < n; i++) {
            arg[0].thread->stats.Merge(arg[i].thread->stats);
        }
        arg[0].thread->stats.Report(name);
        if (is_mixgraph_phase) {
            const uint64_t get_ops = mixgraph_get_ops_.load(std::memory_order_relaxed);
            const uint64_t put_ops = mixgraph_put_ops_.load(std::memory_order_relaxed);
            const uint64_t get_nanos = mixgraph_get_nanos_.load(std::memory_order_relaxed);
            const uint64_t put_nanos = mixgraph_put_nanos_.load(std::memory_order_relaxed);
            const double elapsed_seconds = benchmark_elapsed_us / 1000000.0;
            std::sort(mixgraph_get_latency_nanos_.begin(), mixgraph_get_latency_nanos_.end());
            std::sort(mixgraph_put_latency_nanos_.begin(), mixgraph_put_latency_nanos_.end());
            const std::string phase_name = name.ToString();
            if (get_ops > 0) std::printf("%s_get: ops=%llu ops/sec=%.0f avg_latency_us=%.3f p50_us=%.3f p95_us=%.3f "
                        "p99_us=%.3f found=%llu not_found=%llu hot=%llu cold=%llu hot_ratio=%.2f%% "
                        "hot_regions=%llu/%llu/%llu\n",
                        phase_name.c_str(),
                        static_cast<unsigned long long>(get_ops), elapsed_seconds > 0 ? get_ops / elapsed_seconds : 0,
                        get_ops > 0 ? get_nanos / 1000.0 / get_ops : 0,
                        LatencyPercentileMicros(mixgraph_get_latency_nanos_, 50.0),
                        LatencyPercentileMicros(mixgraph_get_latency_nanos_, 95.0),
                        LatencyPercentileMicros(mixgraph_get_latency_nanos_, 99.0),
                        static_cast<unsigned long long>(mixgraph_get_found_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_get_not_found_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_get_hot_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_get_cold_ops_.load(std::memory_order_relaxed)),
                        get_ops > 0 ? 100.0 * mixgraph_get_hot_ops_.load(std::memory_order_relaxed) / get_ops : 0.0,
                        static_cast<unsigned long long>(mixgraph_get_hot_region_ops_[0].load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_get_hot_region_ops_[1].load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_get_hot_region_ops_[2].load(std::memory_order_relaxed)));
            if (get_ops > 0) std::printf(
                        "%s_get_sources: load_ops=%llu load_not_found=%llu insert_ops=%llu insert_not_found=%llu\n",
                        phase_name.c_str(),
                        static_cast<unsigned long long>(mixgraph_get_load_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(
                            mixgraph_get_load_not_found_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_get_insert_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(
                            mixgraph_get_insert_not_found_ops_.load(std::memory_order_relaxed)));
            if (put_ops > 0) std::printf("%s_put: ops=%llu ops/sec=%.0f avg_latency_us=%.3f p50_us=%.3f p95_us=%.3f "
                        "p99_us=%.3f hot=%llu cold=%llu hot_ratio=%.2f%% hot_regions=%llu/%llu/%llu "
                        "updates=%llu inserts=%llu\n",
                        phase_name.c_str(),
                        static_cast<unsigned long long>(put_ops), elapsed_seconds > 0 ? put_ops / elapsed_seconds : 0,
                        put_ops > 0 ? put_nanos / 1000.0 / put_ops : 0,
                        LatencyPercentileMicros(mixgraph_put_latency_nanos_, 50.0),
                        LatencyPercentileMicros(mixgraph_put_latency_nanos_, 95.0),
                        LatencyPercentileMicros(mixgraph_put_latency_nanos_, 99.0),
                        static_cast<unsigned long long>(mixgraph_put_hot_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_put_cold_ops_.load(std::memory_order_relaxed)),
                        put_ops > 0 ? 100.0 * mixgraph_put_hot_ops_.load(std::memory_order_relaxed) / put_ops : 0.0,
                        static_cast<unsigned long long>(mixgraph_put_hot_region_ops_[0].load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_put_hot_region_ops_[1].load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_put_hot_region_ops_[2].load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_put_update_ops_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(mixgraph_put_insert_ops_.load(std::memory_order_relaxed)));
            if (method == &Benchmark::PutMixGraph) {
                std::sort(mixgraph_inserted_keys_.begin(), mixgraph_inserted_keys_.end(),
                          [](const InsertedKeyRecord& a, const InsertedKeyRecord& b) {
                              return a.physical_key < b.physical_key;
                          });
                std::printf("putmixgraph_inserted_keys: %llu\n",
                            static_cast<unsigned long long>(mixgraph_inserted_keys_.size()));
            }
        }
        if (method == &Benchmark::RangeReadRandom) {
            std::printf("range_scan_summary: queries=%llu entries=%llu bucket_errors=%llu "
                        "range_length=%d elapsed_us=%llu\n",
                        static_cast<unsigned long long>(range_scan_queries_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(range_scan_entries_.load(std::memory_order_relaxed)),
                        static_cast<unsigned long long>(
                            range_scan_bucket_errors_.load(std::memory_order_relaxed)),
                        FLAGS_range_length, static_cast<unsigned long long>(benchmark_elapsed_us));
        }
        if (FLAGS_comparisons) {
            fprintf(stdout, "Comparisons: %zu\n", count_comparator_.comparisons());
            count_comparator_.reset();
            fflush(stdout);
        }

        for (int i = 0; i < n; i++) {
            delete arg[i].thread;
        }
        delete[] arg;
        rdma_mg->Print_Remote_CPU_RPC(0);
        const bool wrote_data = method == &Benchmark::WriteRandom || method == &Benchmark::WriteRandomSharded ||
                                method == &Benchmark::WriteMixGraphKeys || method == &Benchmark::PutMixGraph;
        db_->WaitforAllbgtasks(wrote_data);
        if (wrote_data)
            sleep(3);  // wait for the last sstable disgestion.

        if (method == &Benchmark::ReadRandom || method == &Benchmark::ReadRandom_Sharded) Validation_Read();
    }

    void Crc32c(ThreadState* thread) {
        // Checksum about 500MB of data total
        const int size = 4096;
        const char* label = "(4K per op)";
        std::string data(size, 'x');
        int64_t bytes = 0;
        uint32_t crc = 0;
        while (bytes < 500 * 1048576) {
            crc = crc32c::Value(data.data(), size);
            thread->stats.FinishedSingleOp();
            bytes += size;
        }
        // Print so result is not dead
        std::fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

        thread->stats.AddBytes(bytes);
        thread->stats.AddMessage(label);
    }

    void SnappyCompress(ThreadState* thread) {
        RandomGenerator gen;
        Slice input = gen.Generate(Options().block_size);
        int64_t bytes = 0;
        int64_t produced = 0;
        bool ok = true;
        std::string compressed;
        while (ok && bytes < 1024 * 1048576) {  // Compress 1G
            ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
            produced += compressed.size();
            bytes += input.size();
            thread->stats.FinishedSingleOp();
        }

        if (!ok) {
            thread->stats.AddMessage("(snappy failure)");
        } else {
            char buf[100];
            std::snprintf(buf, sizeof(buf), "(output: %.1f%%)", (produced * 100.0) / bytes);
            thread->stats.AddMessage(buf);
            thread->stats.AddBytes(bytes);
        }
    }

    void SnappyUncompress(ThreadState* thread) {
        RandomGenerator gen;
        Slice input = gen.Generate(Options().block_size);
        std::string compressed;
        bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
        int64_t bytes = 0;
        char* uncompressed = new char[input.size()];
        while (ok && bytes < 1024 * 1048576) {  // Compress 1G
            ok = port::Snappy_Uncompress(compressed.data(), compressed.size(), uncompressed);
            bytes += input.size();
            thread->stats.FinishedSingleOp();
        }
        delete[] uncompressed;

        if (!ok) {
            thread->stats.AddMessage("(snappy failure)");
        } else {
            thread->stats.AddBytes(bytes);
        }
    }

    void Open() {
        assert(db_ == nullptr);
        Options options;
        options.env = g_env;
        options.create_if_missing = !FLAGS_use_existing_db;
        options.block_cache = cache_;
        options.write_buffer_size = FLAGS_write_buffer_size;
        // This workload uses fixed-size values, so translate the requested
        // byte budget into the sequence range used by the normal write path.
        // The allowance covers the internal key, skip-list node, alignment,
        // and arena bookkeeping without letting a 100KB workload overshoot.
        constexpr size_t kEstimatedMemtableEntryOverhead = 256;
        const size_t estimated_memtable_entry_size = static_cast<size_t>(FLAGS_value_size) +
                                                     static_cast<size_t>(FLAGS_key_size) +
                                                     kEstimatedMemtableEntryOverhead;
        options.memtable_max_entries =
            std::max<size_t>(1, options.write_buffer_size / estimated_memtable_entry_size);
        std::printf("Memtable entry limit: %zu (estimated %.2f MiB)\n", options.memtable_max_entries,
                    options.memtable_max_entries * estimated_memtable_entry_size / 1048576.0);
        options.max_file_size = FLAGS_max_file_size;
        options.block_size = FLAGS_block_size;
        options.bloom_bits = FLAGS_bloom_bits;
        options.enable_hot_cold = FLAGS_enable_hot_cold;
        options.block_restart_interval = FLAGS_block_restart_interval;
        if (FLAGS_comparisons) {
            options.comparator = &count_comparator_;
        }
#if TABLE_STRATEGY == 2
        //    options.max_open_files = FLAGS_open_files;

#else
        options.max_open_files = FLAGS_open_files;

#endif
        options.filter_policy = filter_policy_;
        options.reuse_logs = FLAGS_reuse_logs;
        //
        rdma_mg = Env::Default()->rdma_mg.get();
        // TODO: Keep every compute node have 100 million key range
        number_of_key_total = FLAGS_num * FLAGS_threads * rdma_mg->compute_nodes.size();  // whole range.
        number_of_key_per_compute = FLAGS_num * FLAGS_threads;

        //    number_of_key_total = FLAGS_num*FLAGS_threads; // whole range.
        //    number_of_key_per_compute =
        //        number_of_key_total /rdma_mg->compute_nodes.size();

        if (FLAGS_fixed_compute_shards_num > 0) {
            options.ShardInfo = new std::vector<std::pair<Slice, Slice>>();
            number_of_key_per_shard = number_of_key_per_compute / FLAGS_fixed_compute_shards_num;
            for (int i = 0; i < FLAGS_fixed_compute_shards_num; ++i) {
                char* data_low = new char[FLAGS_key_size];
                char* data_up = new char[FLAGS_key_size];
                Slice key_low = Slice(data_low, FLAGS_key_size);
                Slice key_up = Slice(data_up, FLAGS_key_size);
                uint64_t lower_bound =
                    number_of_key_per_compute * (rdma_mg->node_id - 1) / 2 + i * number_of_key_per_shard;
                uint64_t upper_bound =
                    number_of_key_per_compute * (rdma_mg->node_id - 1) / 2 + (i + 1) * number_of_key_per_shard;
                // in case that the number_of_key_per_shard is rounded down.
                if (i == FLAGS_fixed_compute_shards_num - 1) {
                    upper_bound = number_of_key_per_compute * (rdma_mg->node_id + 1) / 2;
                }
                GenerateKeyFromInt(lower_bound, &key_low);
                GenerateKeyFromInt(upper_bound, &key_up);
                options.ShardInfo->emplace_back(key_low, key_up);
            }
        } else if (rdma_mg->memory_nodes.size() > 1) {
            options.ShardInfo = new std::vector<std::pair<Slice, Slice>>();
            number_of_key_per_shard = number_of_key_per_compute / rdma_mg->memory_nodes.size();
            for (int i = 0; i < rdma_mg->memory_nodes.size(); ++i) {
                char* data_low = new char[FLAGS_key_size];
                char* data_up = new char[FLAGS_key_size];
                Slice key_low = Slice(data_low, FLAGS_key_size);
                Slice key_up = Slice(data_up, FLAGS_key_size);
                uint64_t lower_bound =
                    number_of_key_per_compute * (rdma_mg->node_id - 1) / 2 + i * number_of_key_per_shard;
                uint64_t upper_bound =
                    number_of_key_per_compute * (rdma_mg->node_id - 1) / 2 + (i + 1) * number_of_key_per_shard;
                if (i == rdma_mg->memory_nodes.size() - 1) {
                    // in case that the number_of_key_pershard round down.
                    upper_bound = number_of_key_per_compute * (rdma_mg->node_id + 1) / 2;
                }
                GenerateKeyFromInt(lower_bound, &key_low);
                GenerateKeyFromInt(upper_bound, &key_up);
                options.ShardInfo->emplace_back(key_low, key_up);
            }
        }

        Status s = DB::Open(options, FLAGS_db, &db_);
        if (!s.ok()) {
            std::fprintf(stderr, "open error: %s\n", s.ToString().c_str());
            std::exit(1);
        }
    }

    void OpenBench(ThreadState* thread) {
        for (int i = 0; i < num_; i++) {
            delete db_;
            Open();
            thread->stats.FinishedSingleOp();
        }
    }

    void WriteSeq(ThreadState* thread) { DoWrite(thread, true); }

    void WriteRandom(ThreadState* thread) { DoWrite(thread, false); }
    void WriteMixGraphKeys(ThreadState* thread) {
        RandomGenerator gen;
        WriteBatch batch;
        WriteOptions load_options = write_options_;
        load_options.track_hotness = false;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        const uint64_t total_keys = static_cast<uint64_t>(FLAGS_num) * FLAGS_threads;
        MixGraphHotColdLoadMapper hot_cold_mapper(total_keys, key.size());
        int64_t bytes = 0;
        for (int i = 0; i < num_; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; ++j) {
                const uint64_t logical_index = static_cast<uint64_t>(thread->tid) * num_ + i + j;
                const uint64_t ordinal = total_keys - 1 - logical_index;
                const uint64_t physical_key = FLAGS_hot_cold_load
                                                  ? hot_cold_mapper.PhysicalKey(ordinal)
                                                  : MixGraphPhysicalKey(ordinal, total_keys, key.size());
                GenerateKeyFromInt(physical_key, &key);
                batch.Put(key, gen.Generate(value_size_));
                bytes += value_size_ + key.size();
                thread->stats.FinishedSingleOp();
            }
            Status s = db_->Write(load_options, &batch);
            if (!s.ok()) {
                std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                std::exit(1);
            }
        }
        thread->stats.AddBytes(bytes);
    }
    void WriteRandomSharded(ThreadState* thread) { DoWrite_Sharded(thread, false); }
    void Validation_Write() {
        Random64 rand(123);
        RandomGenerator gen;
        Status s;
        std::unique_ptr<const char[]> key_guard;
        WriteBatch batch;
        Slice key = AllocateKey(&key_guard, FLAGS_key_size + 1);
        for (int i = 0; i < 1000; i++) {
            batch.Clear();
            //      //The key range should be adjustable.
            ////        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
            //      const int k = rand.Next()%(FLAGS_num*FLAGS_threads);
            GenerateKeyFromInt(i, &key);
            key.Reset(key.data(), key.size() - 1);
            char to_be_append = 'v';  // add an extra char to make key different from write bench.
            assert(key.size() == FLAGS_key_size);
            key.append(&to_be_append, 1);
            ////      batch.Put(key, gen.Generate(value_size_));
            //      batch.Put(key, key);

            s = db_->Put(write_options_, key, key);
            validation_keys.push_back(key.ToString());
        }
        printf("validation write finished\n");
    }
    void Validation_Read() {
        ReadOptions options;
        // TODO(ruihong): specify the table_cache option.
        std::string value;
        int not_found = 0;
        //    KeyBuffer key;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        for (int i = 0; i < 1000; i++) {
            key = validation_keys[i];
            if (db_->Get(options, key, &value).ok()) {
                if (value != validation_keys[i]) {
                    printf("The fetched value is not correct");
                }
            } else {
                //        printf("Validation failed\n");
                not_found++;
                //        assert(false);
            }
        }
        printf("validation read finished, not found num %d\n", not_found);
    }
    void DoWrite(ThreadState* thread, bool seq) {
        if (num_ != FLAGS_num) {
            char msg[100];
            std::snprintf(msg, sizeof(msg), "(%d ops)", num_);
            thread->stats.AddMessage(msg);
        }

        RandomGenerator gen;
        WriteBatch batch;
        Status s;
        int64_t bytes = 0;
        //    KeyBuffer key;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        for (int i = 0; i < num_; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; j++) {
                // The key range should be adjustable.
                //        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
                const int k = seq ? i + j : thread->rand.Next() % (FLAGS_num * FLAGS_threads);

                //        key.Set(k);
                GenerateKeyFromInt(k, &key);
                //        batch.Put(key.slice(), gen.Generate(value_size_));
                batch.Put(key, gen.Generate(value_size_));
                //        bytes += value_size_ + key.slice().size();
                bytes += value_size_ + key.size();
                thread->stats.FinishedSingleOp();
            }
            s = db_->Write(write_options_, &batch);
            if (!s.ok()) {
                std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                std::exit(1);
            }
        }
        thread->stats.AddBytes(bytes);
    }
    void DoWrite_Sharded(ThreadState* thread, bool seq) {
        if (num_ != FLAGS_num) {
            char msg[100];
            std::snprintf(msg, sizeof(msg), "(%d ops)", num_);
            thread->stats.AddMessage(msg);
        }

        RandomGenerator gen;
        WriteBatch batch;
        Status s;
        int64_t bytes = 0;
        //    KeyBuffer key;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        //    int total_number_of_inserted_key = FLAGS_num*FLAGS_threads;
        uint64_t shard_number_among_computes = (RDMA_Manager::node_id - 1) / 2;
        for (int i = 0; i < num_ * FLAGS_load_vs_num; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; j++) {
                // The key range should be adjustable.
                //         const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
                //        const int k = seq ? i + j : thread->rand.Next()%(number_of_key_per_compute);
                const int k = seq ? i + j : thread->rand.Uniform(number_of_key_per_compute);

                //        key.Set(k);
                GenerateKeyFromInt(k + shard_number_among_computes * number_of_key_per_compute, &key);
                //        batch.Put(key.slice(), gen.Generate(value_size_));
                batch.Put(key, gen.Generate(value_size_));
                //        bytes += value_size_ + key.slice().size();
                bytes += value_size_ + key.size();
                thread->stats.FinishedSingleOp();
            }
            s = db_->Write(write_options_, &batch);
            if (!s.ok()) {
                std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                std::exit(1);
            }
        }
        thread->stats.AddBytes(bytes);
    }
    void ReadSequential(ThreadState* thread) {
        // #ifdef BYTEADDRESSABLE
        //     Iterator* iter = db_->NewSEQIterator(ReadOptions());
        // #endif
        // #ifndef BYTEADDRESSABLE
        Iterator* iter = db_->NewIterator(ReadOptions());
        // #endif
        int i = 0;
        int64_t bytes = 0;
        for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
            bytes += iter->key().size() + iter->value().size();
            thread->stats.FinishedSingleOp();
            ++i;
        }
        delete iter;
        thread->stats.AddBytes(bytes);
    }

    void ReadReverse(ThreadState* thread) {
        Iterator* iter = db_->NewIterator(ReadOptions());
        int i = 0;
        int64_t bytes = 0;
        for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
            bytes += iter->key().size() + iter->value().size();
            thread->stats.FinishedSingleOp();
            ++i;
        }
        delete iter;
        thread->stats.AddBytes(bytes);
    }
    void RangeReadRandom(ThreadState* thread) {
        ReadOptions options;
        uint64_t found = 0;
        uint64_t queries = 0;
        uint64_t bucket_errors = 0;
        std::unique_ptr<const char[]> key_guard1;
        std::unique_ptr<const char[]> key_guard2;
        Slice key_start = AllocateKey(&key_guard1);
        Slice key_end = AllocateKey(&key_guard2);
        uint64_t completed_entries = 0;
        const uint64_t key_space = static_cast<uint64_t>(FLAGS_num) * FLAGS_threads;
        const uint64_t range_length = std::max<uint64_t>(1, std::min<uint64_t>(FLAGS_range_length, key_space));
        Iterator* iter = db_->NewIterator(options);
        DBImpl* db_impl = static_cast<DBImpl*>(db_);
        while (completed_entries < static_cast<uint64_t>(reads_)) {
            const uint64_t entries_requested =
                std::min<uint64_t>(range_length, static_cast<uint64_t>(reads_) - completed_entries);
            const uint64_t max_start = key_space - entries_requested;
            const uint64_t ordinal_start = max_start == 0 ? 0 : thread->rand.Next() % (max_start + 1);
            const uint64_t ordinal_end = ordinal_start + entries_requested;
            const uint64_t physical_start = MixGraphPhysicalKey(ordinal_start, key_space, key_start.size());
            const uint64_t physical_end = MixGraphPhysicalKey(ordinal_end, key_space, key_end.size());
            GenerateKeyFromInt(physical_start, &key_start);
            GenerateKeyFromInt(physical_end, &key_end);
            if (FLAGS_enable_hot_cold && db_impl != nullptr) {
                uint64_t range_entries = 0;
                uint64_t range_bytes = 0;
                uint64_t range_bucket_errors = 0;
                Status s = db_impl->ScanRangeWithBucketHelper(options, iter, key_start, key_end,
                                                              &range_entries, &range_bytes,
                                                              &range_bucket_errors);
                if (!s.ok()) {
                    std::fprintf(stderr, "range scan helper error: %s\n", s.ToString().c_str());
                    std::exit(1);
                }
                found += range_entries;
                bucket_errors += range_bucket_errors;
                thread->stats.AddBytes(range_bytes);
                for (uint64_t entry = 0; entry < range_entries; ++entry) {
                    thread->stats.FinishedSingleOp();
                }
            } else {
                iter->Seek(key_start);
                while (iter->Valid() && iter->key().compare(key_end) < 0) {
                    thread->stats.AddBytes(iter->key().size() + iter->value().size());
                    found++;
                    thread->stats.FinishedSingleOp();
                    iter->Next();
                }
            }
            completed_entries += entries_requested;
            queries++;
        }
        delete iter;
        range_scan_queries_.fetch_add(queries, std::memory_order_relaxed);
        range_scan_entries_.fetch_add(found, std::memory_order_relaxed);
        range_scan_bucket_errors_.fetch_add(bucket_errors, std::memory_order_relaxed);
        char msg[100];
        std::snprintf(msg, sizeof(msg), "(%llu entries in %llu ranges)",
                      static_cast<unsigned long long>(found), static_cast<unsigned long long>(queries));
        thread->stats.AddMessage(msg);
    }
    void ReadRandom(ThreadState* thread) {
        ReadOptions options;
        // TODO(ruihong): specify the table_cache option.
        std::string value;
        int found = 0;
        //    KeyBuffer key;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        for (int i = 0; i < reads_; i++) {
            //      const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);// make it uniform as write.
            const int k = thread->rand.Next() % (FLAGS_num * FLAGS_threads);
            //
            //            key.Set(k);
            GenerateKeyFromInt(k, &key);
            //      if (db_->Get(options, key.slice(), &value).ok()) {
            //        found++;
            //      }
            if (db_->Get(options, key, &value).ok()) {
                found++;
            }
            thread->stats.FinishedSingleOp();
        }
        char msg[100];
        std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
        thread->stats.AddMessage(msg);
    }
    void ReadRandom_Sharded(ThreadState* thread) {
        ReadOptions options;
        // TODO(ruihong): specify the cache option.
        std::string value;
        int found = 0;
        //    KeyBuffer key;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        //    int total_number_of_inserted_key = FLAGS_num*FLAGS_threads;
        uint64_t shard_number_among_computes = (RDMA_Manager::node_id - 1) / 2;
        for (int i = 0; i < reads_; i++) {
            //      const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);// make it uniform as write.
            //      const int k = thread->rand.Next()%(number_of_key_per_compute);
            const int k = thread->rand.Uniform(number_of_key_per_compute);
            //
            //            key.Set(k);
            GenerateKeyFromInt(k + shard_number_among_computes * number_of_key_per_compute, &key);
            //      if (db_->Get(options, key.slice(), &value).ok()) {
            //        found++;
            //      }
            if (db_->Get(options, key, &value).ok()) {
                found++;
            }
            thread->stats.FinishedSingleOp();
        }
        char msg[100];
        std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, reads_);
        thread->stats.AddMessage(msg);
    }
    // This is different from ReadWhileWriting because it does not use
    // an extra thread.
    void ReadRandomWriteRandom(ThreadState* thread) {
        ReadOptions options;
        RandomGenerator gen;
        std::string value;
        int64_t found = 0;
        int get_weight = 0;
        int put_weight = 0;
        int64_t reads_done = 0;
        int64_t writes_done = 0;
        Duration duration(FLAGS_duration, FLAGS_num);

        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        printf("Read percentage is %%%d\n", FLAGS_readwritepercent);
        uint64_t time_elapse;
        // the number of iterations is the larger of read_ or write_
        while (!duration.Done(1)) {
            //      DB* db = SelectDB(thread);
            GenerateKeyFromInt(thread->rand.Next() % (FLAGS_num * FLAGS_threads), &key);
            if (get_weight == 0 && put_weight == 0) {
                // one batch completed, reinitialize for next batch
                get_weight = FLAGS_readwritepercent;
                put_weight = 100 - get_weight;
            }
            if (get_weight > 0) {
                // do all the gets first
                // TODO: remove the time print.
                //        auto start = std::chrono::high_resolution_clock::now();
                Status s = db_->Get(options, key, &value);
                //        auto stop = std::chrono::high_resolution_clock::now();
                //        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
                //        time_elapse+=duration.count();

                if (!s.ok() && !s.IsNotFound()) {
                    fprintf(stderr, "get error: %s\n", s.ToString().c_str());
                    // we continue after error rather than exiting so that we can
                    // find more errors if any
                } else if (!s.IsNotFound()) {
                    found++;
                }
                get_weight--;
                reads_done++;
                thread->stats.FinishedSingleOp();
                //        thread->stats.FinishedOps(nullptr, db, 1, kRead);
            } else if (put_weight > 0) {
                // then do all the corresponding number of puts
                // for all the gets we have done earlier
                Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
                if (!s.ok()) {
                    fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                    exit(1);
                }
                put_weight--;
                writes_done++;
                thread->stats.FinishedSingleOp();
                //        thread->stats.FinishedOps(nullptr, db, 1, kWrite);
            }
        }
        char msg[100];
        snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 " total:%" PRIu64 " found:%" PRIu64 ")",
                 reads_done, writes_done, FLAGS_num, found);
        //    printf("Read average latency is %lu\n", time_elapse/reads_done);
        thread->stats.AddMessage(msg);
    }
    enum class MixGraphMode { kMixed, kPutOnly, kGetOnly };

    void MixGraph(ThreadState* thread) { RunMixGraph(thread, MixGraphMode::kMixed); }
    void PutMixGraph(ThreadState* thread) { RunMixGraph(thread, MixGraphMode::kPutOnly); }
    void GetMixGraph(ThreadState* thread) { RunMixGraph(thread, MixGraphMode::kGetOnly); }

    void RunMixGraph(ThreadState* thread, MixGraphMode mode) {
        // ZippyDB key distribution without range scans: redistribute the
        // original 3% Seek share to Get, yielding 86% Get and 14% Put.
        constexpr int kGetCutoff = 860;
        constexpr int kPutCutoff = 1000;
        constexpr int64_t kMaxScanLength = 10000;
        const uint64_t total_keys = static_cast<uint64_t>(FLAGS_num) * FLAGS_threads;
        MixGraphKeyGenerator key_generator(static_cast<int64_t>(total_keys));
        RandomGenerator values;
        ReadOptions options;
        const int64_t mixgraph_ops = FLAGS_mixgraph_ops < 0 ? reads_ : FLAGS_mixgraph_ops;
        Duration duration(FLAGS_duration, mixgraph_ops);
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        std::string value;
        int64_t gets = 0;
        int64_t puts = 0;
        int64_t seeks = 0;
        int64_t found = 0;
        int64_t not_found = 0;
        int64_t bytes = 0;
        int64_t scanned = 0;
        uint64_t get_nanos = 0;
        uint64_t put_nanos = 0;
        uint64_t get_hot_ops = 0;
        uint64_t get_cold_ops = 0;
        uint64_t get_load_ops = 0;
        uint64_t get_load_not_found_ops = 0;
        uint64_t get_insert_ops = 0;
        uint64_t get_insert_not_found_ops = 0;
        uint64_t put_hot_ops = 0;
        uint64_t put_cold_ops = 0;
        uint64_t put_update_ops = 0;
        uint64_t put_insert_ops = 0;
        std::array<uint64_t, 3> get_hot_region_ops{{0, 0, 0}};
        std::array<uint64_t, 3> put_hot_region_ops{{0, 0, 0}};
        std::vector<uint64_t> get_latency_nanos;
        std::vector<uint64_t> put_latency_nanos;
        std::vector<InsertedKeyRecord> inserted_keys;
        const int64_t expected_gets = mode == MixGraphMode::kGetOnly
                                          ? mixgraph_ops
                                          : (mode == MixGraphMode::kMixed ? mixgraph_ops * 86 / 100 : 0);
        const int64_t expected_puts = mode == MixGraphMode::kPutOnly
                                          ? mixgraph_ops
                                          : (mode == MixGraphMode::kMixed ? mixgraph_ops * 14 / 100 : 0);
        get_latency_nanos.reserve(static_cast<size_t>(std::max<int64_t>(0, expected_gets)));
        put_latency_nanos.reserve(static_cast<size_t>(std::max<int64_t>(0, expected_puts)));
        inserted_keys.reserve(static_cast<size_t>(std::max<int64_t>(0, expected_puts / 2)));

        while (!duration.Done(1)) {
            // Draw the operation and key independently so Get and Put see the
            // same spatial hot/cold distribution rather than correlated bits.
            const uint64_t query_random_value = thread->rand.Next();
            const uint64_t key_random_value = thread->rand.Next();
            const MixGraphKeyGenerator::Sample sample = key_generator.Next(key_random_value, total_keys);
            const int query_type = mode == MixGraphMode::kGetOnly
                                       ? 0
                                       : (mode == MixGraphMode::kPutOnly ? kGetCutoff
                                                                        : static_cast<int>(query_random_value % 1000));
            const double u = static_cast<double>(key_random_value % total_keys) / total_keys;

            if (query_type < kGetCutoff) {
                bool get_inserted_key = mode == MixGraphMode::kGetOnly && !mixgraph_inserted_keys_.empty() &&
                                        ((query_random_value & 1U) != 0);
                bool get_is_hot = sample.is_hot;
                size_t get_hot_region = sample.hot_region;
                if (get_inserted_key) {
                    const InsertedKeyRecord& inserted =
                        mixgraph_inserted_keys_[key_random_value % mixgraph_inserted_keys_.size()];
                    GenerateKeyFromInt(inserted.physical_key, &key);
                    get_is_hot = inserted.is_hot;
                    get_hot_region = inserted.hot_region;
                } else {
                    GenerateKeyFromInt(MixGraphPhysicalKey(sample.key_id, total_keys, key.size()), &key);
                }
                const auto op_start = std::chrono::steady_clock::now();
                Status s = db_->Get(options, key, &value);
                const uint64_t latency_nanos = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                         std::chrono::steady_clock::now() - op_start)
                                                                         .count());
                get_nanos += latency_nanos;
                get_latency_nanos.push_back(latency_nanos);
                if (s.ok()) {
                    ++found;
                    bytes += key.size() + value.size();
                } else if (s.IsNotFound()) {
                    ++not_found;
                    if (get_inserted_key) {
                        ++get_insert_not_found_ops;
                    } else {
                        ++get_load_not_found_ops;
                    }
                } else {
                    std::fprintf(stderr, "mixgraph get error: %s\n", s.ToString().c_str());
                }
                if (get_inserted_key) {
                    ++get_insert_ops;
                } else {
                    ++get_load_ops;
                }
                if (get_is_hot) {
                    ++get_hot_ops;
                    ++get_hot_region_ops[get_hot_region];
                } else {
                    ++get_cold_ops;
                }
                ++gets;
            } else if (query_type < kPutCutoff) {
                // Keep the user-selected 100KB KV size for this experiment.
                // MixGraph still supplies the official operation mix and
                // spatial key distribution; fixed-size updates ensure the hot
                // write stream actually reaches memtable flush/value-zone.
                const int64_t value_size = value_size_;
                const bool insert = (thread->rand.Next() & 1U) != 0;
                if (insert) {
                    const InsertKeySlot insert_slot = NextInsertKeySlot(sample.range_index, total_keys);
                    const uint64_t physical_insert_key =
                        MixGraphPhysicalInsertKey(insert_slot.ordinal, insert_slot.variant, total_keys, key.size());
                    GenerateKeyFromInt(physical_insert_key, &key);
                    inserted_keys.push_back({physical_insert_key, sample.is_hot, sample.hot_region});
                } else {
                    GenerateKeyFromInt(MixGraphPhysicalKey(sample.key_id, total_keys, key.size()), &key);
                }
                const auto op_start = std::chrono::steady_clock::now();
                Status s = db_->Put(write_options_, key, values.Generate(static_cast<size_t>(value_size)));
                const uint64_t latency_nanos = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                         std::chrono::steady_clock::now() - op_start)
                                                                         .count());
                put_nanos += latency_nanos;
                put_latency_nanos.push_back(latency_nanos);
                if (!s.ok()) {
                    std::fprintf(stderr, "mixgraph put error: %s\n", s.ToString().c_str());
                    std::exit(1);
                }
                bytes += key.size() + value_size;
                if (sample.is_hot) {
                    ++put_hot_ops;
                    ++put_hot_region_ops[sample.hot_region];
                } else {
                    ++put_cold_ops;
                }
                if (insert) {
                    ++put_insert_ops;
                } else {
                    ++put_update_ops;
                }
                ++puts;
            } else {
                GenerateKeyFromInt(MixGraphPhysicalKey(sample.key_id, total_keys, key.size()), &key);
                Iterator* iter = db_->NewIterator(options);
                iter->Seek(key);
                int64_t scan_length = MixGraphPareto(u, 0.0, 2.517, 14.236) % kMaxScanLength;
                for (int64_t i = 0; i < scan_length && iter->Valid(); ++i) {
                    bytes += iter->key().size() + iter->value().size();
                    ++scanned;
                    iter->Next();
                }
                if (iter->status().ok()) ++found;
                delete iter;
                ++seeks;
            }
            thread->stats.FinishedSingleOp();
        }

        mixgraph_get_ops_.fetch_add(gets, std::memory_order_relaxed);
        mixgraph_get_found_ops_.fetch_add(found, std::memory_order_relaxed);
        mixgraph_get_not_found_ops_.fetch_add(not_found, std::memory_order_relaxed);
        mixgraph_get_load_ops_.fetch_add(get_load_ops, std::memory_order_relaxed);
        mixgraph_get_load_not_found_ops_.fetch_add(get_load_not_found_ops, std::memory_order_relaxed);
        mixgraph_get_insert_ops_.fetch_add(get_insert_ops, std::memory_order_relaxed);
        mixgraph_get_insert_not_found_ops_.fetch_add(get_insert_not_found_ops, std::memory_order_relaxed);
        mixgraph_put_ops_.fetch_add(puts, std::memory_order_relaxed);
        mixgraph_get_nanos_.fetch_add(get_nanos, std::memory_order_relaxed);
        mixgraph_put_nanos_.fetch_add(put_nanos, std::memory_order_relaxed);
        mixgraph_get_hot_ops_.fetch_add(get_hot_ops, std::memory_order_relaxed);
        mixgraph_get_cold_ops_.fetch_add(get_cold_ops, std::memory_order_relaxed);
        mixgraph_put_hot_ops_.fetch_add(put_hot_ops, std::memory_order_relaxed);
        mixgraph_put_cold_ops_.fetch_add(put_cold_ops, std::memory_order_relaxed);
        mixgraph_put_update_ops_.fetch_add(put_update_ops, std::memory_order_relaxed);
        mixgraph_put_insert_ops_.fetch_add(put_insert_ops, std::memory_order_relaxed);
        for (size_t i = 0; i < get_hot_region_ops.size(); ++i) {
            mixgraph_get_hot_region_ops_[i].fetch_add(get_hot_region_ops[i], std::memory_order_relaxed);
            mixgraph_put_hot_region_ops_[i].fetch_add(put_hot_region_ops[i], std::memory_order_relaxed);
        }
        {
            std::lock_guard<std::mutex> lock(mixgraph_latency_mutex_);
            mixgraph_get_latency_nanos_.insert(mixgraph_get_latency_nanos_.end(), get_latency_nanos.begin(),
                                               get_latency_nanos.end());
            mixgraph_put_latency_nanos_.insert(mixgraph_put_latency_nanos_.end(), put_latency_nanos.begin(),
                                               put_latency_nanos.end());
            mixgraph_inserted_keys_.insert(mixgraph_inserted_keys_.end(), inserted_keys.begin(), inserted_keys.end());
        }

        char msg[220];
        const char* mode_name = mode == MixGraphMode::kPutOnly
                                    ? "put-only"
                                    : (mode == MixGraphMode::kGetOnly ? "get-only" : "mixed");
        std::snprintf(msg, sizeof(msg),
                      "( Gets:%lld Puts:%lld Seek:%lld found:%lld scanned:%lld; MixGraph %s, fixed values)",
                      static_cast<long long>(gets), static_cast<long long>(puts), static_cast<long long>(seeks),
                      static_cast<long long>(found), static_cast<long long>(scanned), mode_name);
        thread->stats.AddBytes(bytes);
        thread->stats.AddMessage(msg);
    }
    void ReadMixGraphKeys(ThreadState* thread) {
        ReadOptions options;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        const uint64_t total_keys = static_cast<uint64_t>(FLAGS_num) * FLAGS_threads;
        std::string value;
        int64_t found = 0;
        int64_t not_found = 0;
        int64_t bytes = 0;
        for (int i = 0; i < reads_; ++i) {
            const uint64_t ordinal = thread->rand.Next() % total_keys;
            GenerateKeyFromInt(MixGraphPhysicalKey(ordinal, total_keys, key.size()), &key);
            Status s = db_->Get(options, key, &value);
            if (s.ok()) {
                ++found;
                bytes += key.size() + value.size();
            } else if (s.IsNotFound()) {
                ++not_found;
            } else {
                std::fprintf(stderr, "mapped validation get error: %s\n", s.ToString().c_str());
            }
            thread->stats.FinishedSingleOp();
        }
        char msg[100];
        std::snprintf(msg, sizeof(msg), "( mapped validation found:%lld not_found:%lld)",
                      static_cast<long long>(found), static_cast<long long>(not_found));
        thread->stats.AddBytes(bytes);
        thread->stats.AddMessage(msg);
    }
    void ReadMissing(ThreadState* thread) {
        ReadOptions options;
        std::string value;
        KeyBuffer key;
        for (int i = 0; i < reads_; i++) {
            const int k = thread->rand.Uniform(FLAGS_num);
            key.Set(k);
            Slice s = Slice(key.slice().data(), key.slice().size() - 1);
            db_->Get(options, s, &value);
            thread->stats.FinishedSingleOp();
        }
    }

    void ReadHot(ThreadState* thread) {
        ReadOptions options;
        std::string value;
        const int range = (FLAGS_num + 99) / 100;
        KeyBuffer key;
        for (int i = 0; i < reads_; i++) {
            const int k = thread->rand.Uniform(range);
            key.Set(k);
            db_->Get(options, key.slice(), &value);
            thread->stats.FinishedSingleOp();
        }
    }

    void SeekRandom(ThreadState* thread) {
        ReadOptions options;
        int found = 0;
        KeyBuffer key;
        for (int i = 0; i < reads_; i++) {
            Iterator* iter = db_->NewIterator(options);
            const int k = thread->rand.Uniform(FLAGS_num);
            key.Set(k);
            iter->Seek(key.slice());
            if (iter->Valid() && iter->key() == key.slice()) found++;
            delete iter;
            thread->stats.FinishedSingleOp();
        }
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
        thread->stats.AddMessage(msg);
    }

    void SeekOrdered(ThreadState* thread) {
        ReadOptions options;
        Iterator* iter = db_->NewIterator(options);
        int found = 0;
        int k = 0;
        KeyBuffer key;
        for (int i = 0; i < reads_; i++) {
            k = (k + (thread->rand.Uniform(100))) % FLAGS_num;
            key.Set(k);
            iter->Seek(key.slice());
            if (iter->Valid() && iter->key() == key.slice()) found++;
            thread->stats.FinishedSingleOp();
        }
        delete iter;
        char msg[100];
        std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
        thread->stats.AddMessage(msg);
    }

    void DoDelete(ThreadState* thread, bool seq) {
        RandomGenerator gen;
        WriteBatch batch;
        Status s;
        KeyBuffer key;
        for (int i = 0; i < num_; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; j++) {
                const int k = seq ? i + j : (thread->rand.Uniform(FLAGS_num));
                key.Set(k);
                batch.Delete(key.slice());
                thread->stats.FinishedSingleOp();
            }
            s = db_->Write(write_options_, &batch);
            if (!s.ok()) {
                std::fprintf(stderr, "del error: %s\n", s.ToString().c_str());
                std::exit(1);
            }
        }
    }

    void DeleteSeq(ThreadState* thread) { DoDelete(thread, true); }

    void DeleteRandom(ThreadState* thread) { DoDelete(thread, false); }

    void ReadWhileWriting(ThreadState* thread) {
        if (thread->tid > 0) {
            ReadRandom(thread);
        } else {
            // Special thread that keeps writing until other threads are done.
            RandomGenerator gen;
            //      KeyBuffer key;
            std::unique_ptr<const char[]> key_guard;

            Slice key = AllocateKey(&key_guard);
            while (true) {
                {
                    MutexLock l(&thread->shared->mu);
                    if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
                        // Other threads have finished
                        break;
                    }
                }

                const int k = thread->rand.Uniform(FLAGS_num * FLAGS_threads);
                //        key.Set(k);
                GenerateKeyFromInt(k, &key);
                Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
                if (!s.ok()) {
                    std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                    std::exit(1);
                }
            }

            // Do not count any of the preceding work/delay in stats.
            thread->stats.Start();
        }
    }

    void ReadWhileWriting_fixnum(ThreadState* thread) {
        static std::atomic<uint64_t> current_range = 0;
        if (thread->tid > 0) {
            ReadOptions options;
            // TODO(ruihong): specify the table_cache option.
            std::string value;
            int found = 0;
            //    KeyBuffer key;
            std::unique_ptr<const char[]> key_guard;
            Slice key = AllocateKey(&key_guard);
            while (thread->shared->num_done.load() == 0) {
                //      const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);// make it uniform as write.
                const int k = thread->rand.Next() % (current_range.load(std::memory_order_relaxed) + PRINT_BATCH);
                //
                //            key.Set(k);
                GenerateKeyFromInt(k, &key);
                //      if (db_->Get(options, key.slice(), &value).ok()) {
                //        found++;
                //      }
                if (db_->Get(options, key, &value).ok()) {
                    thread->stats.batch_found.fetch_add(1);
                }
                thread->stats.FinishedSingleOp();
            }
            char msg[100];
            std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
            thread->stats.AddMessage(msg);
        } else {
            // Special thread that keeps writing until other threads are done.
            RandomGenerator gen;
            //      KeyBuffer key;
            std::unique_ptr<const char[]> key_guard;
            FILE* file = fopen("throughput_batch_print.txt", "a");

            Slice key = AllocateKey(&key_guard);
            for (int i = 0; i < num_; i++) {
                //        {
                //          MutexLock l(&thread->shared->mu);
                //          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
                //            // Other threads have finished
                //            break;
                //          }
                //        }

                const int k = thread->rand.Uniform(PRINT_BATCH) + current_range.load(std::memory_order_relaxed);
                //        key.Set(k);
                GenerateKeyFromInt(k, &key);
                Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
                if (!s.ok()) {
                    std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                    std::exit(1);
                }
                if (i % PRINT_BATCH == 0) {
                    MutexLock l(&thread->shared->mu_1);
                    thread->shared->batch_ready = true;
                    thread->shared->cv_1.Signal();
                    current_range.fetch_add(PRINT_BATCH);
                    printf("current finished put ops is %d\n", i);
                }
            }
            // TODO: pure deletion has bug because the pure deletion makes SSTable only contain keys
            //  without any deltion. the index block will be extremely large.

            // Deletion state.
            //      for (int i = 0; i < num_ ; i++) {
            //        //        {
            //        //          MutexLock l(&thread->shared->mu);
            //        //          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            //        //            // Other threads have finished
            //        //            break;
            //        //          }
            //        //        }
            //
            //        const int k = thread->rand.Uniform(FLAGS_num);
            //        //        key.Set(k);
            //        GenerateKeyFromInt(k, &key);
            //        Status s =
            //            db_->Delete(write_options_, key);
            //        if (!s.ok()) {
            //          std::fprintf(stderr, "delete error: %s\n", s.ToString().c_str());
            //          std::exit(1);
            //        }
            //        if (i%4000000 == 0){
            //          MutexLock l(&thread->shared->mu_1);
            //          thread->shared->batch_ready= true;
            //          thread->shared->cv_1.Signal();
            //          printf("current finished delete ops is %d\n", i);
            //
            //
            //        }
            //      }
            fclose(file);

            // Do not count any of the preceding work/delay in stats.
            thread->stats.Start();
        }
    }

    void Compact(ThreadState* thread) { db_->CompactRange(nullptr, nullptr); }

    void PrintStats(const char* key) {
        std::string stats;
        if (!db_->GetProperty(key, &stats)) {
            stats = "(failed)";
        }
        std::fprintf(stdout, "\n%s\n", stats.c_str());
    }

    static void WriteToFile(void* arg, const char* buf, int n) {
        reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
    }

    void HeapProfile() {
        char fname[100];
        std::snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
        WritableFile* file;
        Status s = g_env->NewWritableFile(fname, &file);
        if (!s.ok()) {
            std::fprintf(stderr, "%s\n", s.ToString().c_str());
            return;
        }
        bool ok = port::GetHeapProfile(WriteToFile, file);
        delete file;
        if (!ok) {
            std::fprintf(stderr, "heap profiling not supported\n");
            g_env->RemoveFile(fname);
        }
    }
};

}  // namespace TimberSaw

int main(int argc, char** argv) {
    for (int i = 1; i < argc; i++) {
        double d;
        int n;
        char junk;
        if (TimberSaw::Slice(argv[i]).starts_with("--benchmarks=")) {
            FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_compression_ratio = d;
        } else if (sscanf(argv[i], "--compute_node_id=%d%c", &n, &junk) == 1) {
            // Set Node id to RDMA manager's static value directly.
            TimberSaw::RDMA_Manager::node_id = 2 * n + 1;
        } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--batchprint=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_batchprint = n;
        } else if (sscanf(argv[i], "--comparisons=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_comparisons = n;
        } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_use_existing_db = n;
        } else if (sscanf(argv[i], "--enable_hot_cold=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_enable_hot_cold = n;
        } else if (sscanf(argv[i], "--hot_cold_load=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_hot_cold_load = n;
        } else if (sscanf(argv[i], "--wait_between_phases=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_wait_between_phases = n;
        } else if (sscanf(argv[i], "--fixed_compute_shards_num=%d%c", &n, &junk) == 1) {
            // if it is zero allocate shard number according to the memory node number
            // if larger than 0 this number should larger than meory node number
            FLAGS_fixed_compute_shards_num = n;
        } else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 && (n == 0 || n == 1)) {
            FLAGS_reuse_logs = n;
        } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
            FLAGS_num = n;
        } else if (sscanf(argv[i], "--mixgraph_ops=%d%c", &n, &junk) == 1) {
            FLAGS_mixgraph_ops = n;
        } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
            FLAGS_reads = n;
        } else if (sscanf(argv[i], "--range_length=%d%c", &n, &junk) == 1 && n > 0) {
            FLAGS_range_length = n;
        } else if (sscanf(argv[i], "--loads=%d%c", &n, &junk) == 1) {
            FLAGS_loads = n;
        } else if (sscanf(argv[i], "--load_vs_num=%d%c", &n, &junk) == 1) {
            FLAGS_load_vs_num = n;
        } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
            FLAGS_value_size = n;
        } else if (sscanf(argv[i], "--key_size=%d%c", &n, &junk) == 1) {
            FLAGS_key_size = n;
        } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
            FLAGS_write_buffer_size = n;
        } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
            FLAGS_max_file_size = n;
        } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
            FLAGS_block_size = n;
        } else if (sscanf(argv[i], "--key_prefix=%d%c", &n, &junk) == 1) {
            FLAGS_key_prefix = n;
        } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
            FLAGS_cache_size = n;
        } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
            FLAGS_bloom_bits = n;
        } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
            FLAGS_open_files = n;
        } else if (sscanf(argv[i], "--numa_awared=%d%c", &n, &junk) == 1) {
            FLAGS_enable_numa = n;
        } else if (sscanf(argv[i], "--block_restart_interval=%d%c", &n, &junk) == 1) {
            FLAGS_block_restart_interval = n;
        } else if (sscanf(argv[i], "--readwritepercent=%d%c", &n, &junk) == 1) {
            FLAGS_readwritepercent = n;
        } else if (sscanf(argv[i], "--duration=%d%c", &n, &junk) == 1) {
            FLAGS_duration = n;
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            FLAGS_db = argv[i] + 5;
        } else {
            std::fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            std::exit(1);
        }
    }
    if (FLAGS_write_buffer_size == 0) FLAGS_write_buffer_size = TimberSaw::Options().write_buffer_size;
    if (FLAGS_max_file_size == 0) FLAGS_max_file_size = TimberSaw::Options().max_file_size;
    if (FLAGS_block_size == 0) FLAGS_block_size = TimberSaw::Options().block_size;
#if TABLE_STRATEGY == 2
//  FLAGS_open_files = TimberSaw::Options().max_open_files;
#else
    FLAGS_open_files = TimberSaw::Options().max_open_files;
#endif
    std::string default_db_path;
    TimberSaw::g_env = TimberSaw::Env::Default();

    // Choose a location for the test database if none given with --db=<path>
    if (FLAGS_db == nullptr) {
        TimberSaw::g_env->GetTestDirectory(&default_db_path);
        default_db_path += "/dbbench";
        FLAGS_db = default_db_path.c_str();
    }

    TimberSaw::Benchmark benchmark;
    benchmark.Run();
    return 0;
}
