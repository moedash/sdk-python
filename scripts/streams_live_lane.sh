#!/usr/bin/env bash
# Runs the streams-over-Nexus live tests against a server built from the stream notifier branch.
#
# The stock dev server has no stream notifier and no Nexus progress, so the lifecycle tests skip
# there. This lane builds a server that has both, starts it with the settings the guide lists,
# and fails if any of those tests skips. Run it from a built sdk-python checkout.
#
# Environment:
#   SERVER_REPO        git URL of the server (default: the moetemp fork)
#   SERVER_REF         branch or tag to build (default: the notifier frontend branch)
#   SERVER_BIN         use this server binary instead of building one
#   PORT_BASE          frontend gRPC port, the other ports follow it (default: 7881)
#   STREAMS_REDIS_URL  Redis to use. Without it, the script starts a scratch redis-server.
#   WORK_DIR           where to build and keep logs (default: a new temporary directory)
set -euo pipefail

SERVER_REPO=${SERVER_REPO:-https://github.com/moetemp/temporal.git}
SERVER_REF=${SERVER_REF:-moe/bridge/n9-notifier-frontend}
PORT_BASE=${PORT_BASE:-7881}
WORK_DIR=${WORK_DIR:-$(mktemp -d -t streams-live-lane)}
SDK_DIR=$(cd "$(dirname "$0")/.." && pwd)

GRPC_PORT=$PORT_BASE
HTTP_PORT=$((PORT_BASE + 1))
REDIS_PORT=$((PORT_BASE + 9))
pids=()

cleanup() {
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

mkdir -p "$WORK_DIR/data"
echo "work dir: $WORK_DIR"

if [[ -z "${SERVER_BIN:-}" ]]; then
  echo "building the server from $SERVER_REPO at $SERVER_REF"
  git clone --quiet --depth 1 --branch "$SERVER_REF" "$SERVER_REPO" "$WORK_DIR/temporal"
  (cd "$WORK_DIR/temporal" && go build -o "$WORK_DIR/temporal-server" ./cmd/server)
  SERVER_BIN="$WORK_DIR/temporal-server"
fi

cat >"$WORK_DIR/dc.yaml" <<EOF
history.enableChasm:
  - value: true
history.enableCHASMCallbacks:
  - value: true
history.enableCHASMSignalBacklinks:
  - value: true
nexusoperation.enableChasmWorkflowOperations:
  - value: true
nexusoperation.chasmWorkflowOperationsRolloutPercent:
  - value: 100
nexusoperation.enableProgress:
  - value: true
nexusoperation.callback.endpoint.template:
  - value: "http://localhost:$HTTP_PORT/namespaces/{{.NamespaceName}}/nexus/callback"
callback.allowedAddresses:
  - value:
      - Pattern: "localhost:$HTTP_PORT"
        AllowInsecure: true
      - Pattern: "127.0.0.1:$HTTP_PORT"
        AllowInsecure: true
system.forceSearchAttributesCacheRefreshOnRead:
  - value: true
streamnotifier.enabled:
  - value: true
history.enableSignalWithStartFromWorkflow:
  - value: true
EOF

sqlite_store() {
  cat <<EOF
      sql:
        pluginName: "sqlite"
        databaseName: "$WORK_DIR/data/temporal.db"
        connectAddr: "localhost"
        connectProtocol: "tcp"
        connectAttributes:
          setup: true
          journal_mode: wal
          synchronous: 2
          cache: "private"
        maxConns: 1
        maxIdleConns: 1
EOF
}

service() {
  cat <<EOF
  $1:
    rpc:
      grpcPort: $2
      membershipPort: $(($2 - 1000))
      bindOnLocalHost: true
EOF
}

{
  cat <<EOF
log:
  stdout: true
  level: warn
persistence:
  defaultStore: sqlite-default
  visibilityStore: sqlite-visibility
  numHistoryShards: 1
  datastores:
    sqlite-default:
$(sqlite_store)
    sqlite-visibility:
$(sqlite_store)
global:
  membership:
    maxJoinDuration: 30s
    broadcastAddress: "127.0.0.1"
services:
$(service frontend "$GRPC_PORT")
      httpPort: $HTTP_PORT
$(service matching $((PORT_BASE + 2)))
$(service history $((PORT_BASE + 3)))
$(service worker $((PORT_BASE + 4)))
clusterMetadata:
  enableGlobalNamespace: false
  failoverVersionIncrement: 10
  masterClusterName: "active"
  currentClusterName: "active"
  clusterInformation:
    active:
      enabled: true
      initialFailoverVersion: 1
      rpcName: "frontend"
      rpcAddress: "localhost:$GRPC_PORT"
      httpAddress: "localhost:$HTTP_PORT"
dcRedirectionPolicy:
  policy: "noop"
dynamicConfigClient:
  filepath: "$WORK_DIR/dc.yaml"
  pollInterval: "10s"
EOF
} >"$WORK_DIR/config.yaml"

"$SERVER_BIN" --config-file "$WORK_DIR/config.yaml" --allow-no-auth start \
  >"$WORK_DIR/server.log" 2>&1 &
pids+=($!)

if [[ -z "${STREAMS_REDIS_URL:-}" ]]; then
  redis-server --port "$REDIS_PORT" --save "" --appendonly no >"$WORK_DIR/redis.log" 2>&1 &
  pids+=($!)
  export STREAMS_REDIS_URL="redis://127.0.0.1:$REDIS_PORT/0"
fi

cd "$SDK_DIR"
export PYTHONPATH="$SDK_DIR${PYTHONPATH:+:$PYTHONPATH}"

# The server takes a few seconds to join its own ring, and the tests need the default namespace.
uv run --no-sync python - "$GRPC_PORT" <<'EOF'
import asyncio
import sys
from datetime import timedelta

from google.protobuf.duration_pb2 import Duration

from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode


async def main() -> None:
    target = f"127.0.0.1:{sys.argv[1]}"
    for _ in range(120):
        try:
            client = await Client.connect(target)
            await client.workflow_service.register_namespace(
                RegisterNamespaceRequest(
                    namespace="default",
                    workflow_execution_retention_period=Duration(
                        seconds=int(timedelta(days=1).total_seconds())
                    ),
                )
            )
            return
        except RPCError as err:
            if err.status == RPCStatusCode.ALREADY_EXISTS:
                return
        except RuntimeError:
            pass
        await asyncio.sleep(1)
    raise SystemExit(f"the server on {target} did not come up")


asyncio.run(main())
EOF
# A new namespace takes up to the namespace cache refresh to reach every service.
sleep 12

status=0
uv run --no-sync pytest \
  tests/contrib/streams \
  tests/nexus/test_workflow_operation_progress.py \
  tests/nexus/test_temporal_system_nexus.py \
  -E "127.0.0.1:$GRPC_PORT" -p no:cacheprovider --junitxml="$WORK_DIR/junit.xml" 2>&1 |
  tee "$WORK_DIR/pytest.log" || status=$?

# A lifecycle test that skips here proves nothing, so a skip for a missing notifier, missing
# progress or missing Redis fails the lane.
gated='no stream notifier|refused Nexus progress|STREAMS_REDIS_URL'
if grep -oE "<skipped [^>]*message=\"[^\"]*($gated)[^\"]*\"" "$WORK_DIR/junit.xml"; then
  echo "live lane: lifecycle tests skipped, see above" >&2
  status=1
fi
exit "$status"
