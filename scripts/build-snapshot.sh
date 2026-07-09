#!/usr/bin/env bash
#
# Builds the worker binary for Linux and bakes it into a Hetzner snapshot via
# Packer. Prints the snapshot ID at the end -- put that in HETZNER_IMAGE.
#
# Prerequisites:
#   - Go toolchain (to cross-compile the worker)
#   - Packer                (https://developer.hashicorp.com/packer/install)
#   - HCLOUD_TOKEN          a Hetzner "Read & Write" API token
#
# Cost note: Packer boots a temporary build server and destroys it when done
# (even on failure), so no build VM is leaked. The finished snapshot itself
# incurs a small monthly storage charge until you delete it.
#
# Architecture: ARCH and SERVER_TYPE must agree -- amd64 with an x86 build
# type (cx22/cpx11), or arm64 with an Ampere type (cax11). The worker VMs
# that boot from the snapshot must be the same architecture family.
#
# Usage:
#   HCLOUD_TOKEN=xxxx ./scripts/build-snapshot.sh
#   HCLOUD_TOKEN=xxxx ARCH=arm64 SERVER_TYPE=cax11 ./scripts/build-snapshot.sh

set -euo pipefail

ARCH="${ARCH:-amd64}"
SERVER_TYPE="${SERVER_TYPE:-cx22}"
LOCATION="${LOCATION:-nbg1}"

cd "$(dirname "$0")/.."

: "${HCLOUD_TOKEN:?set HCLOUD_TOKEN to a Hetzner Read & Write API token}"

echo ">> Building worker binary (linux/${ARCH})..."
mkdir -p dist
CGO_ENABLED=0 GOOS=linux GOARCH="${ARCH}" go build -o dist/worker ./cmd/worker

echo ">> Initializing Packer plugins..."
packer init packer/worker-snapshot.pkr.hcl

echo ">> Baking snapshot (build server: ${SERVER_TYPE} @ ${LOCATION})..."
packer build \
  -var "server_type=${SERVER_TYPE}" \
  -var "location=${LOCATION}" \
  packer/worker-snapshot.pkr.hcl

echo ">> Done. Set HETZNER_IMAGE to the snapshot ID printed above."
