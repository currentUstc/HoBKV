#!/usr/bin/env bash
set -euo pipefail

script_root=/home/k8s/liuyang/dlsm-zippy-cpu-sweep-20260907
experiment_root=${EXPERIMENT_ROOT:-$script_root}
runner="$script_root/run_one.sh"

for mode in baseline hotcold; do
  for cores in 2 4 6 8 10 12 14 16; do
    run_dir="$experiment_root/raw/${mode}-${cores}c"
    if [[ -f "$run_dir/compute.log" ]] &&
       grep -aq 'mixgraph_get:' "$run_dir/compute.log" &&
       grep -aq 'mixgraph_put:' "$run_dir/compute.log"; then
      echo "skip completed ${mode}-${cores}c"
      continue
    fi
    "$runner" "$mode" "$cores"
  done
done
