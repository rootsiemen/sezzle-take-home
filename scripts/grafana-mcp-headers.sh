#!/bin/sh
set -eu

token=$(kubectl -n observability get secret grafana-mcp-server-token \
  -o jsonpath='{.data.token}' | base64 -d)
printf '{"Authorization":"Bearer %s"}\n' "$token"
