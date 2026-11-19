# HoBKV

HoBKV is a hotness-oriented, bucket-indexed key-value store for disaggregated memory. It is a research prototype implemented in C++ on top of [dLSM](https://github.com/anonymousMilk/dLSM).

## Design

HoBKV identifies write-hot key ranges and routes their flushed records to direct-addressed Buckets backed by a shared Value Zone on the memory node. Bucket slot positions preserve key order without repeatedly merging those hot records through SST levels. Cold ranges, and ranges without a suitable order-preserving integer key mapping, continue to use the SST-based LSM path.

The implementation extends dLSM with hot-range identification, Bucket indexing, a shared Value Zone, hot/cold transitions, and RDMA data paths. See the accompanying paper for the design, assumptions, and evaluation methodology.

## Requirements

HoBKV targets Linux systems with CMake, a C++ compiler, RDMA-capable NICs, `libibverbs`, and NUMA support. The compute and memory nodes must have compatible network and RDMA configurations. The project includes GoogleTest and Google Benchmark as Git submodules.

## Build

Clone the repository and its submodules, then build with CMake:

```bash
git clone --recurse-submodules https://github.com/currentUstc/HoBKV.git
cd HoBKV
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DWITH_GFLAGS=1
cmake --build build --target Server db_bench TimberSaw -j
```

Install the platform development packages required by CMake first, including the RDMA and NUMA development libraries.

## Configure and run

Create a local `connection.conf` containing the compute-node and memory-node addresses expected by the deployment, one per line. This machine-specific file is ignored by Git and should not be committed. Start the memory-node service with:

```bash
./build/Server TCPIPPORT MEMORYSIZE NODEID
```

Then run a benchmark on a configured compute node. For example:

```bash
./build/db_bench --benchmarks=fillrandom,readrandom \
  --threads=16 --value_size=400 --num=100000000 \
  --bloom_bits=10 --compute_node_id=0 --fixed_compute_shards_num=0
```

Benchmark options and resource limits depend on the cluster configuration. Review the source and paper before using this research prototype with valuable data.

## Evaluation artifacts

The `reports/` directory contains experiment notes and scripts. Cluster addresses must be supplied through environment variables such as `NODE4` and `NODE6`; do not put private addresses or credentials in tracked files.

## Attribution and license

HoBKV is built on the dLSM codebase and retains its existing third-party components and license notices. The repository's `LICENSE` file is BSD 3-Clause; consult it and the individual third-party licenses for redistribution terms.
