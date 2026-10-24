#!/usr/bin/env bash
# Hardened bootstrap for a FRESH Ubuntu 24.04 VPS (Black Pine Lab).
#
# Run once as root on a newly provisioned image — never on a host that was
# compromised. Idempotent: safe to re-run after a partial failure.
#
# Inputs (environment only — nothing is hardcoded, nothing is logged):
#   ADMIN_USER         required  login for the new sudo user (e.g. bpl)
#   ADMIN_PUBKEY       required  ssh public key line (or ADMIN_PUBKEY_FILE=path)
#   TS_AUTHKEY         optional  Tailscale auth key (one-off, pre-approved, tagged).
#                                When set and `tailscale up` succeeds, public SSH
#                                is closed and SSH is reachable on tailscale0 only.
#   TS_HOSTNAME        optional  tailnet hostname (default: current hostname)
#   KEEP_PUBLIC_SSH    optional  1 = leave port 22 open publicly even with Tailscale
#
#   ADMIN_USER=bpl ADMIN_PUBKEY="$(cat ~/.ssh/bpl_ed25519.pub)" TS_AUTHKEY=tskey-... \
#     ssh root@NEW_IP 'bash -s' < ops/bootstrap/harden.sh
#   (see ops/README.md for the exact invocation)
#
# Keep your current SSH session open until you have verified a NEW login works.

set -Eeuo pipefail
IFS=$'\n\t'

LOG=/var/log/bpl-harden.log
exec > >(tee -a "$LOG") 2>&1
log() { printf '%s [%s] %s\n' "$(date -u +%FT%TZ)" "$1" "$2"; }
trap 'log ERROR "failed at line $LINENO: $BASH_COMMAND (re-run is safe)"' ERR

[[ $EUID -eq 0 ]] || { log ERROR "run as root"; exit 1; }
. /etc/os-release
[[ "${ID:-}" == "ubuntu" ]] || { log ERROR "Ubuntu only (got ${ID:-unknown})"; exit 1; }
[[ "${VERSION_ID:-}" == "24.04" ]] || log WARN "tested on 24.04, got ${VERSION_ID:-?}"

: "${ADMIN_USER:?ADMIN_USER is required}"
[[ "$ADMIN_USER" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || { log ERROR "invalid ADMIN_USER"; exit 1; }
if [[ -z "${ADMIN_PUBKEY:-}" && -n "${ADMIN_PUBKEY_FILE:-}" ]]; then
    ADMIN_PUBKEY="$(<"$ADMIN_PUBKEY_FILE")"
fi
: "${ADMIN_PUBKEY:?ADMIN_PUBKEY or ADMIN_PUBKEY_FILE is required}"
[[ "$ADMIN_PUBKEY" =~ ^(ssh-ed25519|sk-ssh-ed25519@openssh.com|ecdsa-sha2-nistp256|sk-ecdsa-sha2-nistp256@openssh.com)\  ]] \
    || { log ERROR "ADMIN_PUBKEY must be an ed25519/ecdsa (or FIDO sk-) public key"; exit 1; }
TS_AUTHKEY="${TS_AUTHKEY:-}"
KEEP_PUBLIC_SSH="${KEEP_PUBLIC_SSH:-0}"
export DEBIAN_FRONTEND=noninteractive

log INFO "bootstrap start (user=$ADMIN_USER tailscale=$([[ -n $TS_AUTHKEY ]] && echo yes || echo no))"

# --- 1. Packages --------------------------------------------------------------
log INFO "apt: full upgrade + baseline packages"
apt-get update -q
apt-get -yq -o Dpkg::Options::=--force-confold full-upgrade
apt-get -yq install ca-certificates curl gnupg ufw fail2ban unattended-upgrades \
    apt-listchanges auditd apparmor apparmor-utils needrestart debsums jq

install -m 0755 -d /etc/apt/keyrings

# Docker CE from Docker's signed repo (not the distro's docker.io, not get.docker.com|sh).
if [[ ! -f /etc/apt/keyrings/docker.asc ]]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
fi
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list

# Tailscale from its signed repo.
if [[ ! -f /usr/share/keyrings/tailscale-archive-keyring.gpg ]]; then
    curl -fsSL "https://pkgs.tailscale.com/stable/ubuntu/${VERSION_CODENAME}.noarmor.gpg" \
        -o /usr/share/keyrings/tailscale-archive-keyring.gpg
fi
curl -fsSL "https://pkgs.tailscale.com/stable/ubuntu/${VERSION_CODENAME}.tailscale-keyring.list" \
    -o /etc/apt/sources.list.d/tailscale.list

apt-get update -q
apt-get -yq install docker-ce docker-ce-cli containerd.io docker-compose-plugin tailscale

# --- 2. Admin user, root lockdown ----------------------------------------------
log INFO "admin user + root lockdown"
if ! id "$ADMIN_USER" &>/dev/null; then
    useradd -m -s /bin/bash -G sudo "$ADMIN_USER"
fi
passwd -l "$ADMIN_USER" >/dev/null   # key-only; sudo below needs no password
install -d -m 0700 -o "$ADMIN_USER" -g "$ADMIN_USER" "/home/$ADMIN_USER/.ssh"
printf '%s\n' "$ADMIN_PUBKEY" > "/home/$ADMIN_USER/.ssh/authorized_keys"
chown "$ADMIN_USER:$ADMIN_USER" "/home/$ADMIN_USER/.ssh/authorized_keys"
chmod 0600 "/home/$ADMIN_USER/.ssh/authorized_keys"
echo "$ADMIN_USER ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/90-$ADMIN_USER"
chmod 0440 "/etc/sudoers.d/90-$ADMIN_USER"
visudo -cf "/etc/sudoers.d/90-$ADMIN_USER" >/dev/null
passwd -l root >/dev/null
# Provider images often seed root keys; root login is disabled below anyway.
: > /root/.ssh/authorized_keys 2>/dev/null || true

# Dedicated non-login service account for the trading/signal stack.
id bpl-svc &>/dev/null || useradd -r -m -d /srv/bpl -s /usr/sbin/nologin bpl-svc
install -d -m 0750 -o bpl-svc -g bpl-svc /srv/bpl
install -d -m 0700 -o bpl-svc -g bpl-svc /etc/bpl   # runtime secrets land here, never in git

# --- 3. SSH ---------------------------------------------------------------------
log INFO "sshd hardening"
cat > /etc/ssh/sshd_config.d/10-bpl-hardening.conf <<EOF
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
AuthenticationMethods publickey
PermitEmptyPasswords no
AllowUsers $ADMIN_USER
MaxAuthTries 3
LoginGraceTime 20
MaxStartups 10:30:60
ClientAliveInterval 300
ClientAliveCountMax 2
X11Forwarding no
AllowAgentForwarding no
AllowTcpForwarding no
PermitTunnel no
LogLevel VERBOSE
EOF
# cloud-init drop-ins can re-enable passwords; neutralise them.
for f in /etc/ssh/sshd_config.d/*.conf; do
    [[ "$f" == */10-bpl-hardening.conf ]] && continue
    sed -i -E 's/^\s*(PasswordAuthentication|PermitRootLogin|KbdInteractiveAuthentication)\b/# bpl-disabled: &/' "$f"
done
sshd -t
systemctl restart ssh

# --- 4. Tailscale ------------------------------------------------------------------
TS_OK=0
if [[ -n "$TS_AUTHKEY" ]]; then
    log INFO "tailscale up"
    systemctl enable --now tailscaled
    # Pass the key via a 0600 file so it never appears in the process list.
    TS_KEYFILE="$(mktemp)"; chmod 0600 "$TS_KEYFILE"
    printf '%s' "$TS_AUTHKEY" > "$TS_KEYFILE"
    if tailscale up --auth-key="file:$TS_KEYFILE" --hostname="${TS_HOSTNAME:-$(hostname)}" \
           --ssh=false --accept-dns=false; then
        TS_IP="$(tailscale ip -4 | head -1)"
        [[ -n "$TS_IP" ]] && TS_OK=1 && log INFO "tailnet ip: $TS_IP"
    else
        log WARN "tailscale up failed — public SSH stays open"
    fi
    shred -u "$TS_KEYFILE" 2>/dev/null || rm -f "$TS_KEYFILE"
fi
unset TS_AUTHKEY

# --- 5. Firewall -------------------------------------------------------------------
log INFO "ufw: default deny inbound"
ufw --force reset >/dev/null
ufw default deny incoming
ufw default allow outgoing
ufw default deny routed
if [[ $TS_OK -eq 1 ]]; then
    ufw allow in on tailscale0 to any port 22 proto tcp comment 'ssh via tailnet'
    ufw allow 41641/udp comment 'tailscale direct'
    if [[ "$KEEP_PUBLIC_SSH" == "1" ]]; then
        ufw limit 22/tcp comment 'public ssh (rate-limited)'
    fi
else
    ufw limit 22/tcp comment 'public ssh (rate-limited)'
fi
ufw logging low
ufw --force enable

# --- 6. Docker: never publish to 0.0.0.0 by accident ------------------------------
# Docker writes its own iptables rules and bypasses ufw; a stray "5432:5432"
# would be internet-facing. Default every published port to loopback.
log INFO "docker daemon hardening"
install -d -m 0755 /etc/docker
cat > /etc/docker/daemon.json <<'EOF'
{
  "ip": "127.0.0.1",
  "no-new-privileges": true,
  "live-restore": true,
  "userland-proxy": false,
  "icc": false,
  "log-driver": "json-file",
  "log-opts": { "max-size": "20m", "max-file": "5" }
}
EOF
systemctl enable docker
systemctl restart docker
# Do NOT add ADMIN_USER or bpl-svc to the docker group: docker group == root.

# --- 7. Kernel / network sysctls -------------------------------------------------
log INFO "sysctl hardening"
cat > /etc/sysctl.d/90-bpl-hardening.conf <<'EOF'
net.ipv4.conf.all.rp_filter = 1
net.ipv4.conf.default.rp_filter = 1
net.ipv4.conf.all.accept_redirects = 0
net.ipv4.conf.default.accept_redirects = 0
-net.ipv6.conf.all.accept_redirects = 0
net.ipv4.conf.all.send_redirects = 0
net.ipv4.conf.all.accept_source_route = 0
net.ipv4.conf.all.log_martians = 1
net.ipv4.tcp_syncookies = 1
net.ipv4.icmp_echo_ignore_broadcasts = 1
kernel.kptr_restrict = 2
kernel.dmesg_restrict = 1
-kernel.yama.ptrace_scope = 2
-kernel.unprivileged_bpf_disabled = 1
fs.protected_symlinks = 1
fs.protected_hardlinks = 1
fs.suid_dumpable = 0
EOF
sysctl --system >/dev/null || log WARN "some sysctls not applied (see: sysctl --system)"

# --- 8. fail2ban -----------------------------------------------------------------
cat > /etc/fail2ban/jail.d/bpl.local <<'EOF'
[DEFAULT]
bantime = 1h
bantime.increment = true
findtime = 10m
maxretry = 4
backend = systemd

[sshd]
enabled = true
EOF
systemctl enable --now fail2ban
systemctl restart fail2ban

# --- 9. Automatic security updates -----------------------------------------------
cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
APT::Periodic::AutocleanInterval "7";
EOF
cat > /etc/apt/apt.conf.d/52bpl-unattended <<'EOF'
Unattended-Upgrade::Origins-Pattern {
    "origin=${distro_id},archive=${distro_codename}-security";
    "origin=Docker";
    "origin=Tailscale";
};
Unattended-Upgrade::Remove-Unused-Dependencies "true";
// No surprise reboots on a trading host: needrestart flags pending reboots,
// take them in a planned maintenance window.
Unattended-Upgrade::Automatic-Reboot "false";
EOF
systemctl enable --now unattended-upgrades

# --- 10. Audit: tripwires on the persistence paths used last time ------------------
log INFO "auditd rules"
cat > /etc/audit/rules.d/90-bpl.rules <<'EOF'
-w /etc/passwd -p wa -k identity
-w /etc/shadow -p wa -k identity
-w /etc/group -p wa -k identity
-w /etc/sudoers -p wa -k priv
-w /etc/sudoers.d/ -p wa -k priv
-w /etc/ssh/sshd_config -p wa -k sshd
-w /etc/ssh/sshd_config.d/ -p wa -k sshd
-w /root/.ssh/ -p wa -k sshkeys
-w /etc/ld.so.preload -p wa -k preload
-w /etc/crontab -p wa -k cron
-w /etc/cron.d/ -p wa -k cron
-w /var/spool/cron/ -p wa -k cron
-w /etc/systemd/system/ -p wa -k systemd
-w /etc/docker/ -p wa -k docker
-w /etc/bpl/ -p rwa -k bpl_secrets
-a always,exit -F arch=b64 -S execve -F dir=/tmp -k exec_tmp
-a always,exit -F arch=b64 -S execve -F dir=/dev/shm -k exec_tmp
-a always,exit -F arch=b64 -S execve -F dir=/var/tmp -k exec_tmp
EOF
augenrules --load >/dev/null
systemctl enable --now auditd

# --- 11. Mount /tmp and /dev/shm noexec ------------------------------------------
if ! grep -qE '^\S+\s+/dev/shm\s' /etc/fstab; then
    echo 'tmpfs /dev/shm tmpfs defaults,noexec,nosuid,nodev 0 0' >> /etc/fstab
fi
mount -o remount,noexec,nosuid,nodev /dev/shm || log WARN "could not remount /dev/shm"
install -d /etc/systemd/system/tmp.mount.d
cat > /etc/systemd/system/tmp.mount.d/options.conf <<'EOF'
[Mount]
Options=mode=1777,strictatime,nosuid,nodev,noexec,size=50%
EOF
systemctl daemon-reload
systemctl enable tmp.mount >/dev/null 2>&1 || true

# --- 12. Integrity baseline --------------------------------------------------------
log INFO "integrity baseline"
debsums -s 2>/dev/null > /var/log/bpl-debsums-baseline.txt || true
find /usr/bin /usr/sbin /usr/local/bin -type f -exec sha256sum {} + \
    > /var/lib/bpl-bin-baseline.sha256 2>/dev/null || true
chmod 0600 /var/lib/bpl-bin-baseline.sha256

# --- Summary -----------------------------------------------------------------------
log INFO "done"
ufw status verbose
echo
echo "NEXT (from your laptop, in a NEW terminal — keep this session open):"
if [[ $TS_OK -eq 1 ]]; then
    echo "  ssh $ADMIN_USER@$TS_IP           # via tailnet"
    [[ "$KEEP_PUBLIC_SSH" == "1" ]] || echo "  public :22 is CLOSED. Lock it at the provider firewall too."
else
    echo "  ssh $ADMIN_USER@<public-ip>      # public, key-only, rate-limited"
    echo "  Re-run with TS_AUTHKEY set to move SSH onto the tailnet."
fi
echo "  then: sudo reboot   (applies kernel updates + tmp.mount)"
