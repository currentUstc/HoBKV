#!/usr/bin/env bash
set -euo pipefail

mode=${1:?usage: run_range.sh baseline|hotcold}
case "$mode" in
  baseline) hot_cold=0 ;;
  hotcold) hot_cold=1 ;;
  *) echo "invalid mode: $mode" >&2; exit 2 ;;
esac

node4=${NODE4:?Set NODE4 to the memory-node address}
node6=${NODE6:?Set NODE6 to the compute-node address}
remote_root=${REMOTE_ROOT:-/tmp/dlsm-hotcold-20260906-1349}
server_bin=${SERVER_BIN:-$remote_root/build/Server.bucket-scan-mvcc-v35}
compute_bin=${COMPUTE_BIN:-$remote_root/build/db_bench.bucket-scan-mvcc-v35}
result_root=${RESULT_ROOT:-/home/k8s/liuyang/dlsm-zippy-cpu-sweep-20260907/range}
run_dir="$result_root/$mode-10c"
mkdir -p "$run_dir"

ssh4=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$node4")
ssh6=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/tmp/codex-dlsm-known-hosts "$node6")
scp4=(scp -q -o BatchMode=yes -o ConnectTimeout=10)
scp6=(scp -q -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/tmp/codex-dlsm-known-hosts)

server_log="$remote_root/range-${mode}-server.log"
compute_log="$remote_root/range-${mode}-compute.log"
node4_cpu_log="$remote_root/range-${mode}-node4-cpu.log"
node6_cpu_log="$remote_root/range-${mode}-node6-cpu.log"
core_list=1,3,5,7,9,11,13,15,17,19

if "${ssh4[@]}" "ss -ltn | grep -q ':19843 '"; then
  echo "node4 port 19843 is already in use" >&2
  exit 1
fi

printf '%s\n' \
  "mode=$mode" "enable_hot_cold=$hot_cold" "server_cores=10" \
  "server_core_list=$core_list" "frontend_threads=16" "load_entries=300000" \
  "put_operations=160000" "value_size=100000" "write_buffer_size=33554432" \
  "range_length=64" "range_entries_requested_per_thread=640" > "$run_dir/config.txt"

server_pid=$("${ssh4[@]}" \
  "cd '$remote_root/build' && nohup numactl --physcpubind='$core_list' '$server_bin' 19843 88 0 > '$server_log' 2>&1 < /dev/null & echo \$!")
cleanup() {
  "${ssh6[@]}" "test -n '${compute_pid:-}' && kill -TERM '${compute_pid:-0}' 2>/dev/null || true" || true
  "${ssh4[@]}" "kill -TERM '$server_pid' 2>/dev/null || true" || true
}
trap cleanup ERR INT TERM

for _ in $(seq 1 60); do
  "${ssh4[@]}" "ss -ltn | grep -q ':19843 '" && break
  sleep 1
done
"${ssh4[@]}" "ss -ltn | grep -q ':19843 '"
"${ssh4[@]}" "nohup pidstat -h -u -r -p '$server_pid' 1 > '$node4_cpu_log' 2>&1 < /dev/null &"

compute_cmd="'$compute_bin' --benchmarks=fillmixgraph,putmixgraph,readrandomrange --num=18750 \
--threads=16 --value_size=100000 --write_buffer_size=33554432 --mixgraph_ops=10000 \
--reads=640 --range_length=64 --enable_hot_cold=$hot_cold --wait_between_phases=1 \
--histogram=1 --compute_node_id=0"
compute_pid=$("${ssh6[@]}" \
  "cd '$remote_root/build' && nohup stdbuf -oL -eL sh -c \"exec $compute_cmd\" > '$compute_log' 2>&1 < /dev/null & echo \$!")
"${ssh6[@]}" "nohup pidstat -h -u -r -p '$compute_pid' 1 > '$node6_cpu_log' 2>&1 < /dev/null &"

deadline=$((SECONDS + 1200))
while "${ssh6[@]}" "kill -0 '$compute_pid' 2>/dev/null"; do
  (( SECONDS < deadline )) || { echo "range run timed out" >&2; exit 1; }
  sleep 1
done

"${scp6[@]}" "$node6:$compute_log" "$run_dir/compute.log"
"${scp6[@]}" "$node6:$node6_cpu_log" "$run_dir/node6-cpu.log"
"${scp6[@]}" "$node6:/tmp/TimberSawtest-1002/dbbench/LOG" "$run_dir/db.log" || true
"${scp4[@]}" "$node4:$server_log" "$run_dir/server.log"
"${scp4[@]}" "$node4:$node4_cpu_log" "$run_dir/node4-cpu.log"

grep -aq 'background_wait_after_fillmixgraph: end' "$run_dir/compute.log"
grep -aq 'background_wait_after_putmixgraph: end' "$run_dir/compute.log"
grep -aq 'range_scan_summary:' "$run_dir/compute.log"
trap - ERR INT TERM
echo "completed range $mode"
