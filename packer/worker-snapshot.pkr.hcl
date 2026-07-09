# Bakes the worker binary into a Hetzner snapshot so worker VMs boot fast
# (design §4.2: "create servers from a pre-baked snapshot containing the
# worker binary"). The snapshot ID this produces goes in HETZNER_IMAGE.
#
# Packer boots a temporary build server, uploads the binary, installs the
# runtime deps, snapshots it, and DESTROYS the build server -- including on
# failure. That auto-teardown is why this uses Packer rather than a
# hand-rolled script: a build VM left running is exactly the cost leak the
# project's guardrails exist to prevent.
#
# Run via scripts/build-snapshot.sh (which compiles dist/worker first).

packer {
  required_plugins {
    hcloud = {
      source  = "github.com/hetznercloud/hcloud"
      version = ">= 1.6.0"
    }
  }
}

variable "hcloud_token" {
  type      = string
  sensitive = true
  default   = env("HCLOUD_TOKEN")
}

# The build server's type. Must match the ARCH of dist/worker and of the
# worker VMs that will boot from this snapshot: use an x86 type (cx22/cpx11)
# for a linux/amd64 binary, or an Ampere type (cax11) for linux/arm64. A
# snapshot's architecture is fixed at build time.
variable "server_type" {
  type    = string
  default = "cx22"
}

variable "location" {
  type    = string
  default = "nbg1"
}

variable "base_image" {
  type    = string
  default = "ubuntu-24.04"
}

# Path to the pre-compiled static worker binary (built by build-snapshot.sh).
variable "worker_binary" {
  type    = string
  default = "dist/worker"
}

source "hcloud" "worker" {
  token         = var.hcloud_token
  image         = var.base_image
  location      = var.location
  server_type   = var.server_type
  ssh_username  = "root"
  snapshot_name = "crawler-worker-{{timestamp}}"
  snapshot_labels = {
    role = "worker-snapshot"
  }
}

build {
  sources = ["source.hcloud.worker"]

  # The cloud-init the orchestrator injects at boot writes the systemd unit
  # and env file itself (they carry per-run values like RUN_ID), so the
  # snapshot only needs the binary present at the path that unit's ExecStart
  # points to.
  provisioner "file" {
    source      = var.worker_binary
    destination = "/usr/local/bin/worker"
  }

  provisioner "shell" {
    inline = [
      "set -eu",
      "chmod +x /usr/local/bin/worker",
      "test -x /usr/local/bin/worker",
      "export DEBIAN_FRONTEND=noninteractive",
      "apt-get update",
      # ca-certificates: TLS to Postgres/object storage/HTTPS crawl targets.
      # curl: used by the self-destruct script the orchestrator's cloud-init
      # installs to delete the VM at its TTL.
      "apt-get install -y ca-certificates curl",
      "apt-get clean",
      "rm -rf /var/lib/apt/lists/*",
    ]
  }
}
