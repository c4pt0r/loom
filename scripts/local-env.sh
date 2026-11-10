#!/bin/sh
# Use this workspace's isolated Colima installation if present; otherwise preserve
# the caller's Docker Desktop / Colima / remote Docker configuration.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
local_work="$root/../../work"
if [ -z "${DOCKER_HOST:-}" ] && [ -z "${DOCKER_CONTEXT:-}" ] && [ -S "$local_work/colima/loom/docker.sock" ]; then
    local_work=$(CDPATH= cd -- "$local_work" && pwd)
    export DOCKER_HOST="unix://$local_work/colima/loom/docker.sock"
    export DOCKER_CONFIG="${DOCKER_CONFIG:-$local_work/docker-config}"
fi
exec "$@"
