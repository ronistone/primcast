#!/bin/bash

STATS_SECS=1
BUILD="target/debug"
OPT="$@"
OUTSTANDING=1
GLOBAL_DESTS=2
GLOBALS=0.0
SINGLE_THREAD=""
CHECK=0

CONFIG="example_3_groups.yaml"
GIDS=3
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
            CMD="$VG ./$BUILD/examples/closed_loop \
                      --gid $g --pid $i --cfg $CONFIG \
                      --global-dests $GLOBAL_DESTS --globals $GLOBALS \
                      -o $OUTSTANDING --stats $STATS_SECS $SINGLE_THREAD"
            if (( CHECK != 0 )); then
                CMD="$CMD --check > out_${g}_${i}.txt"
            fi
            tmux send-keys -t $(( $g * $GIDS + $i + $FIRST_PANE_NUMBER )) "$CMD" C-m
        done
    done

    tmux attach-session -t primcast
}

usage () {
    echo "$0 [--release] [--cfg CFG_FILE] [-o OUTSTANDING] [--globals PERCENT] [--global-dests N] [--help]"
    exit 1
}

while [[ $# > 0 ]]; do
    key="$1"
    case $key in
        --release)
            BUILD="target/release"
            ;;
        -o)
            OUTSTANDING=$2
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
        --single-thread)
            SINGLE_THREAD="--single-thread"
            ;;
        --check)
            CHECK=1
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
