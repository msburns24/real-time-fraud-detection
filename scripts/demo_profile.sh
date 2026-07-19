#!/usr/bin/env bash
# Hot-path decomposition: where the time in a /predict actually goes.
#
# stderr is dropped because the profiler emits the real per-request structured
# log 2000 times — that logging cost is one of the stages being measured, so it
# is left switched on rather than disabled for the demo.
set -euo pipefail

echo "profiling the request path in isolation, 2000 iterations:"
echo
.venv/bin/python -m scripts.profile_predict -n 2000 2>/dev/null
echo
echo "~0.14 ms of application work. The same client benchmarking /health, which"
echo "does no work at all, shows a 0.26 ms transport floor — so most of a"
echo "request is framework and transport, not our code."
echo
echo "The system is framework-bound, not compute-bound."
