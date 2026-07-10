# Deploying to Hetzner

One always-on VM runs Postgres + the orchestrator (see cost/architecture
discussion in project history for why they're combined). Worker VMs are
ephemeral, created and destroyed per run by `HetznerProvisioner`.

## 1. Create the private network (once)

Worker VMs need to reach this VM's Postgres without exposing it publicly.
They keep their own public IP for crawl IP-diversity; the private network
is additive, not a replacement (see `HetznerConfig.PrivateNetworkID`).

```
hcloud network create --name crawler-net --ip-range 10.0.0.0/16
hcloud network add-subnet crawler-net --network-zone eu-central --type cloud --ip-range 10.0.1.0/24
```

Note the network ID -> `HETZNER_PRIVATE_NETWORK_ID`.

## 2. Create the always-on VM

```
hcloud server create --name orchestrator \
  --type cx23 --image ubuntu-24.04 --location nbg1 \
  --network crawler-net \
  --ssh-key <your-key-name>
```

Note this VM's private IP (`hcloud server describe orchestrator`) -> used
for `POSTGRES_BIND_ADDR` below, since workers reach Postgres at
`<this-private-ip>:5432`, not `localhost` or a Docker-internal hostname.

## 3. Lock down the firewall

Only SSH should ever be reachable on this VM's public interface — Postgres
binds to the private IP only, so it's never on the public interface at all.

```
hcloud firewall create --name orchestrator-fw \
  --rule 'direction=in,protocol=tcp,port=22,source_ips=<your-ip>/32'
hcloud firewall apply-to-resource orchestrator-fw --type server --server orchestrator
```

## 4. Install Docker on the VM

```
ssh root@<vm-public-ip>
curl -fsSL https://get.docker.com | sh
```

## 5. Deploy the code

```
git clone <this-repo> /opt/crawler-orchestrator
cd /opt/crawler-orchestrator
cp .env.example .env   # fill in real values -- see notes below
```

`.env` values that differ from local dev:
- `PROVISIONER=hetzner`
- `POSTGRES_BIND_ADDR=<this VM's private IP>` (not `127.0.0.1`)
- `DB_URL=postgres://<user>:<pass>@<this VM's private IP>:5432/<db>?sslmode=disable`
  (both the orchestrator and every worker use this same value)
- `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` -> your Hetzner
  Object Storage endpoint/credentials, not self-hosted MinIO
- `HETZNER_API_TOKEN`, `HETZNER_SERVER_TYPE`, `HETZNER_LOCATION`,
  `HETZNER_PRIVATE_NETWORK_ID` (from step 1)
- `HETZNER_IMAGE` -> the snapshot ID from `scripts/build-snapshot.sh`
  (build this from your workstation before deploying, not on this VM)

## 6. Apply migrations, then bring up Postgres

```
docker compose -f deploy/docker-compose.prod.yaml up -d postgres
docker compose -f deploy/docker-compose.prod.yaml --profile manual run --rm migrate
```

## 7. Build the orchestrator image and do one manual run

Verify everything end-to-end before trusting the timer with it:

```
docker compose -f deploy/docker-compose.prod.yaml --profile manual run --rm orchestrator
```

Watch its logs; confirm it reconciles, sizes, provisions, monitors to
drain, and tears down cleanly.

## 8. Install the daily timer

```
sudo cp deploy/crawler-orchestrator.service deploy/crawler-orchestrator.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crawler-orchestrator.timer
```

Adjust `OnCalendar` in `crawler-orchestrator.timer` to your desired daily
run time before enabling, if `02:00:00` UTC doesn't fit.

## Verifying it's running

```
systemctl status crawler-orchestrator.timer
journalctl -u crawler-orchestrator.service -f
```
