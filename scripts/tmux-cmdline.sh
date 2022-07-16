#!/bin/bash

STATS_SECS=10
BUILD="target/debug"
CONFIG="example_2_groups.yaml"
OPT="$@"
OUTSTANDING=1
GLOBAL_DESTS=2
GLOBALS=0.0


tmux_test ()  {
    tmux new-session -d -s primcast
    tmux new-window -t primcast

    for (( i = 0; i < 5; i++ )); do
        tmux split
        tmux select-layout even-vertical
    done

    FIRST_PANE_NUMBER=`tmux list-panes | head -1 | cut -d':' -f1`

    for (( g = 0; g < 2; g++ )); do
        for (( i = 0; i < 3; i++ )); do
            CMD="$VG ./$BUILD/examples/cmdline \
                      --gid $g --pid $i --cfg $CONFIG"
            tmux send-keys -t $(( $g * 3 + $i + $FIRST_PANE_NUMBER )) "$CMD" C-m
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
