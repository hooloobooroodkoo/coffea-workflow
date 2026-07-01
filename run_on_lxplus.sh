#!/bin/bash
set -e
voms-proxy-init --voms cms --valid 192:00
apptainer exec ./worker.sif python3 run_workflow.py
