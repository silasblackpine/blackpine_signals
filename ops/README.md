# ops/

Host-level tooling. Nothing here contains or reads secret values.

## incident/triage.sh — read-only forensic triage

Run from the provider's **rescue image** against the compromised disk mounted
read-only. Never on the live compromised OS unless rescue boot is impossible
(`--live`, output untrusted).

```bash
mkdir -p /mnt/target
mount -o ro,noexec,nodev,nosuid /dev/sdX1 /mnt/target   # lsblk to find it
bash triage.sh /mnt/target /root/triage-out 30           # 30 = days back
scp /root/triage-*.tar.gz you@clean-host:                # get it off the box
```

Collects accounts, SSH keys, persistence (cron, systemd, `ld.so.preload`, PAM,
shell init), recently modified binaries, `/tmp` droppers, miner/botnet IOCs,
auth logs, Docker container configs, and an inventory of secret **names** (not
values) found in `.env` files — that list is your rotation checklist.
Outputs are SHA-256 sealed into a tarball.

Read first: `uid0-accounts`, `authorized-keys`, `ld-preload`, `cron`,
`systemd-units-recent`, `bin-modified`, `tmp-files`, `miner-botnet-iocs`,
`auth-accepted`, `dpkg-verify`.

## bootstrap/harden.sh — fresh-VPS hardening

For a **new** Ubuntu 24.04 instance only. Idempotent; logs to
`/var/log/bpl-harden.log`.

```bash
# from your laptop; values come from your shell, never from a file in git
ssh root@NEW_IP "ADMIN_USER=bpl ADMIN_PUBKEY='$(cat ~/.ssh/bpl_ed25519.pub)' \
  TS_AUTHKEY='$TS_AUTHKEY' bash -s" < ops/bootstrap/harden.sh
# keep that session open; in a new terminal:
ssh bpl@<tailnet-ip>
```

Does: full upgrade; Docker CE + Tailscale from signed repos; key-only SSH with
root login disabled, restricted to `tailscale0` when `TS_AUTHKEY` is given;
ufw default-deny; Docker publishes to `127.0.0.1` by default (Docker bypasses
ufw); `no-new-privileges`; sysctl hardening; fail2ban; unattended security
upgrades (no auto-reboot); auditd tripwires on the usual persistence paths;
`noexec` `/tmp` and `/dev/shm`; binary hash baseline; a `bpl-svc` service
account with `/etc/bpl` (0700) for runtime secrets.

Use a one-off, pre-approved, tagged Tailscale auth key. Do not add any user to
the `docker` group (equivalent to root).
