#!/usr/bin/env bash
set -euo pipefail

: "${OCI_STREAM_ID:?Set OCI_STREAM_ID}"
: "${OCI_STREAM_ENDPOINT:?Set OCI_STREAM_ENDPOINT}"

exec python3 oci_stream_load.py both \
  --duration 600 \
  --message-rate 3000 \
  --message-bytes 256 \
  --batch-size 100 \
  --producer-workers 4 \
  --consumer-workers 2 \
  --get-limit 10000
