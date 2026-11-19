#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 baseline|hotcold SERVER_CORES" >&2
  exit 2
fi

mode=$1
server_cores=$2
case "$mode" in
  baseline) hot_cold=0 ;;
  hotcold) hot_cold=1 ;;
  *) echo "invalid mode: $mode" >&2; exit 2 ;;
esac

if (( server_cores < 2 || server_cores > 16 || server_cores % 2 != 0 )); then
  echo "SERVER_CORES must be an even number from 2 to 16" >&2
  exit 2
fi

node4=${NODE4:?Set NODE4 to the memory-node address}
node6=${NODE6:?Set NODE6 to the compute-node address}
ssh4=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$node4")
ssh6=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/tmp/codex-dlsm-known-hosts "$node6")
scp4=(scp -q -o BatchMode=yes -o ConnectTimeout=10)
scp6=(scp -q -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/tmp/codex-dlsm-known-hosts)

experiment_root=${EXPERIMENT_ROOT:-/home/k8s/liuyang/dlsm-zippy-cpu-sweep-20260907}
run_suffix=${RUN_SUFFIX:-}
run_name="${mode}-${server_cores}c${run_suffix}"
run_dir="$experiment_root/raw/$run_name"
mkdir -p "$run_dir"

remote_root=/tmp/dlsm-hotcold-20260906-1349
server_bin="$remote_root/build/Server.concurrent-read-v22"
compute_bin="$remote_root/build/db_bench.lock-scope-v26"
server_log="$remote_root/sweep-server-${run_name}.log"
compute_log="$remote_root/sweep-compute-${run_name}.log"
node4_cpu_log="$remote_root/sweep-cpu-${run_name}-node4.log"
node6_cpu_log="$remote_root/sweep-cpu-${run_name}-node6.log"

physical_cores=(1 3 5 7 9 11 13 15 17 19 21 23 25 27 29 31)
core_list=$(IFS=,; echo "${physical_cores[*]:0:server_cores}")

if "${ssh4[@]}" "ss -ltn | grep -q ':19843 '"; then
  echo "node4 port 19843 is already in use" >&2
  exit 1
fi
if "${ssh6[@]}" "pgrep -f '^$compute_bin' >/dev/null"; then
  echo "node6 already has a sweep db_bench process" >&2
  exit 1
fi

printf 'event\tepoch_seconds\nstart\t%s\n' "$(date +%s.%N)" > "$run_dir/timeline.tsv"
printf '%s\n' \
  "mode=$mode" \
  "enable_hot_cold=$hot_cold" \
  "server_cores=$server_cores" \
  "server_core_list=$core_list" \
  "frontend_threads=16" \
  "load_entries=300000" \
  "put_operations=160000" \
  "get_operations=160000" \
  "mixed_operations=160000" \
  "mixed_get_percent=86" \
  "mixed_put_percent=14" \
  "value_size=100000" \
  "write_buffer_size=33554432" \
  "wait_between_phases=1" > "$run_dir/config.txt"

server_pid=$("${ssh4[@]}" \
  "cd '$remote_root/build' && nohup numactl --physcpubind='$core_list' '$server_bin' 19843 88 0 > '$server_log' 2>&1 < /dev/null & echo \$!")

cleanup() {
  "${ssh6[@]}" "test -n '${compute_pid:-}' && kill -TERM '${compute_pid:-0}' 2>/dev/null || true" || true
  "${ssh4[@]}" "kill -TERM '$server_pid' 2>/dev/null || true" || true
}
trap cleanup ERR INT TERM

for _ in $(seq 1 60); do
  if "${ssh4[@]}" "ss -ltn | grep -q ':19843 '"; then
    break
  fi
  sleep 1
done
if ! "${ssh4[@]}" "ss -ltn | grep -q ':19843 '"; then
  echo "Server did not listen on port 19843" >&2
  exit 1
fi

"${ssh4[@]}" "nohup pidstat -h -u -r -p '$server_pid' 1 > '$node4_cpu_log' 2>&1 < /dev/null &"

compute_cmd="'$compute_bin' --benchmarks=fillmixgraph,putmixgraph,getmixgraph,mixgraph --num=18750 --threads=16 --value_size=100000 --write_buffer_size=33554432 --mixgraph_ops=10000 --enable_hot_cold=$hot_cold --wait_between_phases=1 --histogram=1 --compute_node_id=0"
compute_pid=$("${ssh6[@]}" \
  "cd '$remote_root/build' && nohup stdbuf -oL -eL sh -c \"exec $compute_cmd\" > '$compute_log' 2>&1 < /dev/null & echo \$!")
"${ssh6[@]}" "nohup pidstat -h -u -r -p '$compute_pid' 1 > '$node6_cpu_log' 2>&1 < /dev/null &"

seen_fill=0
seen_put=0
seen_get=0
seen_mix=0
deadline=$((SECONDS + 1200))
while "${ssh6[@]}" "kill -0 '$compute_pid' 2>/dev/null"; do
  if (( SECONDS >= deadline )); then
    echo "run timed out" >&2
    exit 1
  fi
  if (( seen_fill == 0 )) && "${ssh6[@]}" "grep -aq 'background_wait_after_fillmixgraph: end' '$compute_log'"; then
    printf 'load_background_end_put_start\t%s\n' "$(date +%s.%N)" >> "$run_dir/timeline.tsv"
    seen_fill=1
  fi
  if (( seen_put == 0 )) && "${ssh6[@]}" "grep -aq 'background_wait_after_putmixgraph: end' '$compute_log'"; then
    printf 'put_background_end_get_start\t%s\n' "$(date +%s.%N)" >> "$run_dir/timeline.tsv"
    seen_put=1
  fi
  if (( seen_get == 0 )) && "${ssh6[@]}" "grep -aq 'getmixgraph_get:' '$compute_log'"; then
    printf 'get_end_mix_start\t%s\n' "$(date +%s.%N)" >> "$run_dir/timeline.tsv"
    seen_get=1
  fi
  if (( seen_mix == 0 )) && "${ssh6[@]}" "grep -aq 'mixgraph_get:' '$compute_log'"; then
    seen_mix=1
  fi
  sleep 1
done
printf 'end\t%s\n' "$(date +%s.%N)" >> "$run_dir/timeline.tsv"

for _ in $(seq 1 30); do
  if ! "${ssh4[@]}" "kill -0 '$server_pid' 2>/dev/null"; then
    break
  fi
  sleep 1
done

"${scp6[@]}" "$node6:$compute_log" "$run_dir/compute.log"
"${scp6[@]}" "$node6:$node6_cpu_log" "$run_dir/node6-cpu.log"
"${scp6[@]}" "$node6:/tmp/TimberSawtest-1002/dbbench/LOG" "$run_dir/db.log" || true
"${scp4[@]}" "$node4:$server_log" "$run_dir/server.log"
"${scp4[@]}" "$node4:$node4_cpu_log" "$run_dir/node4-cpu.log"

if ! grep -aq 'fillmixgraph:' "$run_dir/compute.log" ||
   ! grep -aq 'background_wait_after_fillmixgraph: end' "$run_dir/compute.log" ||
   ! grep -aq 'putmixgraph_put:' "$run_dir/compute.log" ||
   ! grep -aq 'background_wait_after_putmixgraph: end' "$run_dir/compute.log" ||
   ! grep -aq 'getmixgraph_get:' "$run_dir/compute.log" ||
   ! grep -aq 'mixgraph_get:' "$run_dir/compute.log" ||
   ! grep -aq 'mixgraph_put:' "$run_dir/compute.log"; then
  echo "run did not complete every requested phase" >&2
  exit 1
fi

trap - ERR INT TERM
echo "completed $run_name"
