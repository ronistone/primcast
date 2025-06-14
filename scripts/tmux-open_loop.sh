#!/bin/bash

STATS_SECS=1
BUILD="target/debug"
OPT="$@"
MSGS_SEC=1
GLOBAL_DESTS=1
GLOBALS=0.0
THREADS=""
CHECK=0
DEBUG=""
HYBRID=""

CONFIG="example_1_groups.yaml"
GIDS=1
PIDS=3


tmux_test ()  {
    tmux new-session -d -s primcast
    tmux new-window -t primcast

    for (( i = 0; i < (GIDS * PIDS) - 1; i++ )); do
        tmux split
        tmux select-layout even-vertical
    done

    FIRST_PANE_NUMBER=`tmux list-panes | head -1 | cut -d':' -f1`

    for (( g = 0; g < $GIDS; g++ )); do
        for (( i = 0; i < $PIDS; i++ )); do
            CMD="$VG ./$BUILD/examples/open_loop \
                      --gid $g --pid $i --cfg $CONFIG \
                      --global-dests $GLOBAL_DESTS --globals $GLOBALS \
                      -m $MSGS_SEC --stats $STATS_SECS $THREADS $HYBRID $DEBUG"
            if (( CHECK != 0 )); then
                CMD="$CMD --check"
            fi
            CMD="$CMD  > out_${g}_${i}.txt 2> log_out_${g}_${i}.txt"
            tmux send-keys -t $(( $g * $PIDS + $i + $FIRST_PANE_NUMBER )) "$CMD" C-m
        done
    done

    tmux attach-session -t primcast
}

usage () {
    echo "$0 [--release] [-o OUTSTANDING] [--globals PERCENT] [--global-dests N] [--threads N] [--help]"
    exit 1
}

while [[ $# > 0 ]]; do
    key="$1"
    case $key in
        --release)
            BUILD="target/release"
            ;;
        -m)
            MSGS_SEC=$2
            shift
            ;;
        --globals)
            GLOBALS=$2
            shift
            ;;
        --global-dests)
            GLOBAL_DESTS=$2
            shift
            ;;
        --threads)
            THREADS="--threads $2"
            shift
            ;;
        --hybrid)
            HYBRID="--hybrid"
            ;;
        --check)
            CHECK=1
            ;;
        --debug)
            DEBUG="--debug $2"
            shift
            ;;
        -c|--config)
            CONFIG=$2
            shift
            ;;
        -g|--gids)
            GIDS=$2
            shift
            ;;
        -p|--pids)
            PIDS=$2
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            usage
            ;;
    esac
    shift
done

tmux_test
