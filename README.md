## Example executables

There are two very similar examples under `primcast-net/examples`:
- `closed_loop ... -o N`: generates load in a closed loop with N outstanding requests.
- `open_loop.rs ... -m N`: generates load in an open loop, submitting N msgs per second.

Both accept the following parameters:
- `--gid GID --pid PID`
- `--cfg FILE`: yaml config file
- `--global-dests N`: "size" of global commands, default is 2
- `--globals F`: fraction of global commands from 0.0 to 1.0
- `--stats S`: gather and print statistics every S seconds
- `--hybrid`: hybrid logical clocks
- `--threads N`: by default, replica runs in a single-threaded executor. This option start a multi-threaded one.

## Notes

Leader election and recovery not implemented/tested.
Pid 0 at each group is always the primary.

It seems that running more than 2 threads doesn't benefit the benchmarks much.
The primary seems to benefit from being multithreaded under high load, and it is best to pin the process into a couple of CPUS (NUMA close):

    taskset 0x5 closed_loop ... --threads 2

## Checking the protocol for replica consistency

1) Go to root project folder.
2) Build with `cargo build --release --examples`
3) Run `scripts/tmux-open_loop.sh` or `scripts/tmux-closed_loop.sh` with the `--release` and `--check` option. For example:

    ./scripts/tmux-open_loop.sh --globals 0.5 -m 1000 --release --check

4) Let it run for a bit. Kill the processes.
   One `out_GID_PID.txt` file will be created per process.
   Deliveries will be printed one per line in order (grep for DELIVERY).
5) Run `scripts/check_consistency.sh`


