#!/usr/bin/env bash

tmux new-session -d -s scheduler "dask-scheduler"
tmux split-window "dask-worker localhost:8786 --nprocs 4"
tmux attach

