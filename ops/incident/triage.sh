#!/usr/bin/env bash
# Read-only forensic triage of a (suspected) compromised Linux host.
#
# Intended use: boot the provider's RESCUE image (not the compromised OS),
# mount the compromised root filesystem READ-ONLY, then run:
#
#   mount -o ro,noexec,nodev,nosuid /dev/sdX1 /mnt/target
#   ./triage.sh /mnt/target /root/triage-out
#
# --live mode inspects the running system instead. Only use it if rescue
# boot is impossible: on a compromised OS, ps/ss/ls themselves may lie.
#
# The script never writes to the target, never prints secret VALUES
# (only env var NAMES and file paths), and keeps going when a step fails.

set -uo pipefail
IFS=$'\n\t'

usage() {
    echo "usage: $0 [--live] <target-root> <output-dir> [days-back=30]" >&2
    exit 2
}

LIVE=0
if [[ "${1:-}" == "--live" ]]; then LIVE=1; shift; fi
[[ $# -ge 2 ]] || usage

ROOT="$(realpath -m "$1")"
OUT="$(realpath -m "$2")"
DAYS="${3:-30}"
[[ "$DAYS" =~ ^[0-9]+$ ]] || usage

if [[ "$ROOT" == "/" && $LIVE -eq 0 ]]; then
    echo "refusing: target is / — boot rescue and mount the disk, or pass --live" >&2
    exit 2
fi
[[ -d "$ROOT/etc" ]] || { echo "no $ROOT/etc — wrong mount point?" >&2; exit 2; }
case "$OUT/" in "$ROOT"/*) [[ "$ROOT" != "/" ]] && { echo "output must not be inside target" >&2; exit 2; } ;; esac

if [[ $LIVE -eq 0 ]] && findmnt -no OPTIONS --target "$ROOT" 2>/dev/null | grep -qw rw; then
    echo "WARNING: $ROOT is mounted read-write. Remount ro first: mount -o remount,ro $ROOT" >&2
    read -r -p "continue anyway? [y/N] " ans
    [[ "$ans" == "y" ]] || exit 1
fi

umask 077
mkdir -p "$OUT"
LOG="$OUT/triage.log"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SINCE="$(date -u -d "-${DAYS} days" +%Y-%m-%d)"

log() { printf '%s [%s] %s\n' "$(date -u +%FT%TZ)" "$1" "$2" | tee -a "$LOG" >&2; }

# run <name> <cmd...>: capture stdout+stderr to $OUT/<name>.txt, never abort.
run() {
    local name="$1"; shift
    log INFO "collect: $name"
    if ! "$@" >"$OUT/$name.txt" 2>&1; then
        log WARN "step '$name' exited non-zero (partial output kept)"
    fi
}

# Restrict find to the target filesystem; never descend into proc/sys.
tfind() { find "$@" -xdev -not -path "$ROOT/proc/*" -not -path "$ROOT/sys/*" 2>/dev/null; }

log INFO "triage start root=$ROOT out=$OUT since=$SINCE live=$LIVE"

# --- System identity --------------------------------------------------------
run os-release        cat "$ROOT/etc/os-release"
run hostname          cat "$ROOT/etc/hostname"
run hosts             cat "$ROOT/etc/hosts"
run resolv            cat "$ROOT/etc/resolv.conf"

# --- Accounts & privilege ---------------------------------------------------
run uid0-accounts     awk -F: '$3==0' "$ROOT/etc/passwd"
run login-shells      awk -F: '$7 !~ /(nologin|false|sync|halt|shutdown)$/' "$ROOT/etc/passwd"
run passwd-mtime      stat -c '%y %n' "$ROOT/etc/passwd" "$ROOT/etc/shadow" "$ROOT/etc/group" "$ROOT/etc/sudoers"
run shadow-hashed     awk -F: '$2 ~ /^\$/ {print $1}' "$ROOT/etc/shadow"
run sudoers           bash -c "cat '$ROOT/etc/sudoers'; for f in '$ROOT'/etc/sudoers.d/*; do echo \"== \$f\"; cat \"\$f\"; done"

# --- SSH ------------------------------------------------------------------
run sshd-config       bash -c "cat '$ROOT/etc/ssh/sshd_config'; for f in '$ROOT'/etc/ssh/sshd_config.d/*; do echo \"== \$f\"; cat \"\$f\"; done"
run authorized-keys   bash -c "for f in '$ROOT'/root/.ssh/authorized_keys* '$ROOT'/home/*/.ssh/authorized_keys*; do [ -f \"\$f\" ] || continue; echo \"== \$f (\$(stat -c %y \"\$f\"))\"; cat \"\$f\"; done"
run ssh-private-keys  bash -c "find '$ROOT/root' '$ROOT/home' -xdev -type f \\( -name 'id_*' ! -name '*.pub' -o -name '*.pem' -o -name '*.key' \\) -printf '%TY-%Tm-%Td %p\n' 2>/dev/null"

# --- Persistence ------------------------------------------------------------
run ld-preload        bash -c "cat '$ROOT/etc/ld.so.preload' 2>/dev/null || echo '(none — good)'"
run cron              bash -c "for f in '$ROOT'/etc/crontab '$ROOT'/etc/cron.d/* '$ROOT'/var/spool/cron/crontabs/* '$ROOT'/var/spool/cron/*; do [ -f \"\$f\" ] || continue; echo \"== \$f (\$(stat -c %y \"\$f\"))\"; cat \"\$f\"; done; ls -la '$ROOT'/etc/cron.{hourly,daily,weekly,monthly}"
run systemd-units-recent bash -c "find '$ROOT/etc/systemd' '$ROOT/lib/systemd/system' '$ROOT/usr/lib/systemd/system' '$ROOT'/home/*/.config/systemd '$ROOT/root/.config/systemd' -xdev -type f -newermt '$SINCE' -printf '%TY-%Tm-%Td %TH:%TM %p\n' 2>/dev/null | sort"
run systemd-enabled   bash -c "find '$ROOT/etc/systemd/system' -name '*.wants' -type d -exec ls -la {} \\; 2>/dev/null"
run systemd-timers    bash -c "find '$ROOT/etc/systemd' '$ROOT/lib/systemd' '$ROOT/usr/lib/systemd' -name '*.timer' 2>/dev/null"
run rc-local          bash -c "cat '$ROOT/etc/rc.local' 2>/dev/null || echo '(none)'"
run shell-init        bash -c "for f in '$ROOT'/etc/profile '$ROOT'/etc/profile.d/* '$ROOT'/etc/bash.bashrc '$ROOT'/root/.bashrc '$ROOT'/root/.profile '$ROOT'/home/*/.bashrc '$ROOT'/home/*/.profile; do [ -f \"\$f\" ] || continue; echo \"== \$f (\$(stat -c %y \"\$f\"))\"; grep -nE 'curl|wget|base64|nohup|/tmp/|/dev/shm|LD_PRELOAD|\\beval\\b' \"\$f\"; done"
run pam-recent        bash -c "find '$ROOT/etc/pam.d' '$ROOT/lib/x86_64-linux-gnu/security' '$ROOT/usr/lib/x86_64-linux-gnu/security' -xdev -newermt '$SINCE' -printf '%TY-%Tm-%Td %p\n' 2>/dev/null"
run kernel-modules    bash -c "cat '$ROOT'/etc/modules '$ROOT'/etc/modules-load.d/* 2>/dev/null; find '$ROOT/lib/modules' -xdev -name '*.ko*' -newermt '$SINCE' 2>/dev/null"

# --- Binaries & suspicious files -------------------------------------------
run bin-modified      bash -c "find '$ROOT'/bin '$ROOT'/sbin '$ROOT'/usr/bin '$ROOT'/usr/sbin '$ROOT'/usr/local/bin '$ROOT'/usr/local/sbin -xdev -type f -newermt '$SINCE' -printf '%TY-%Tm-%Td %TH:%TM %m %p\n' 2>/dev/null | sort"
run suid-sgid         bash -c "find '$ROOT' -xdev -not -path '$ROOT/proc/*' -type f \\( -perm -4000 -o -perm -2000 \\) -printf '%TY-%Tm-%Td %m %u %p\n' 2>/dev/null | sort"
run tmp-files         bash -c "find '$ROOT/tmp' '$ROOT/var/tmp' '$ROOT/dev/shm' -xdev \\( -type f -o -type d -name '.*' \\) -printf '%TY-%Tm-%Td %TH:%TM %s %m %p\n' 2>/dev/null | sort"
run hidden-exec-home  bash -c "find '$ROOT/root' '$ROOT/home' '$ROOT/var/lib' '$ROOT/opt' -xdev -type f -perm -u+x -path '*/.*' -newermt '$SINCE' -printf '%TY-%Tm-%Td %s %p\n' 2>/dev/null | grep -vE '/\\.(git|cache/pip|npm|nvm|cargo|rustup|local/lib)/' | sort"
run recent-changes-etc bash -c "find '$ROOT/etc' -xdev -type f -newermt '$SINCE' -printf '%TY-%Tm-%Td %TH:%TM %p\n' 2>/dev/null | sort"
run miner-botnet-iocs bash -c "grep -rIlE 'stratum\\+tcp|xmrig|kinsing|kdevtmpfsi|cryptonight|minexmr|nanopool|supportxmr|c3pool|pool\\.hashvault|mirai|gafgyt|tsunami' '$ROOT/etc' '$ROOT/tmp' '$ROOT/var/tmp' '$ROOT/root' '$ROOT/home' '$ROOT/opt' '$ROOT/usr/local' '$ROOT/var/spool' 2>/dev/null | head -200"

if [[ -x "$(command -v dpkg)" && -d "$ROOT/var/lib/dpkg" ]]; then
    # Files whose content differs from the installed package (?M5 = md5 mismatch).
    run dpkg-verify   bash -c "dpkg --root='$ROOT' --verify 2>/dev/null | grep -v ' c /' "
fi

# --- Logs -------------------------------------------------------------------
run auth-accepted     bash -c "zgrep -hE 'Accepted (password|publickey|keyboard)' '$ROOT'/var/log/auth.log* 2>/dev/null | tail -500"
run auth-failed-top   bash -c "zgrep -hoE 'Failed password for (invalid user )?[^ ]+ from [0-9a-f.:]+' '$ROOT'/var/log/auth.log* 2>/dev/null | awk '{print \$NF}' | sort | uniq -c | sort -rn | head -50"
run auth-sudo         bash -c "zgrep -hE 'sudo:.*COMMAND|useradd|usermod|passwd\\[|groupadd' '$ROOT'/var/log/auth.log* 2>/dev/null | tail -300"
run wtmp              bash -c "last -F -f '$ROOT/var/log/wtmp' 2>/dev/null | head -200"
run btmp-count        bash -c "lastb -f '$ROOT/var/log/btmp' 2>/dev/null | wc -l"
if [[ -d "$ROOT/var/log/journal" ]]; then
    run journal-ssh   journalctl --directory="$ROOT/var/log/journal" --since "$SINCE" -u ssh -u sshd --no-pager -o short-iso
    run journal-errs  bash -c "journalctl --directory='$ROOT/var/log/journal' --since '$SINCE' -p warning --no-pager -o short-iso | tail -3000"
fi
run shell-histories   bash -c "for f in '$ROOT'/root/.*_history '$ROOT'/home/*/.*_history; do [ -f \"\$f\" ] || continue; echo \"== \$f (\$(stat -c %y \"\$f\"))\"; tail -300 \"\$f\"; done"

# --- Docker -----------------------------------------------------------------
DOCKER="$ROOT/var/lib/docker"
if [[ -d "$DOCKER" ]]; then
    run docker-containers bash -c "for c in '$DOCKER'/containers/*/config.v2.json; do python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d.get(\"Name\"), d[\"Config\"].get(\"Image\"), d.get(\"Created\"), \" \".join(d.get(\"Path\",\"\").split()+d.get(\"Args\",[])) [:300])' \"\$c\"; done 2>/dev/null"
    run docker-port-binds bash -c "for c in '$DOCKER'/containers/*/hostconfig.json; do echo \"== \$c\"; python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get(\"PortBindings\"))' \"\$c\"; done 2>/dev/null"
    run docker-images     bash -c "python3 -c 'import json,sys; [print(k) for k in json.load(open(sys.argv[1]))[\"Repositories\"]]' '$DOCKER/image/overlay2/repositories.json' 2>/dev/null"
    run docker-daemon     bash -c "cat '$ROOT/etc/docker/daemon.json' 2>/dev/null || echo '(none)'"
fi

# --- Network exposure config -----------------------------------------------
run listening-config  bash -c "cat '$ROOT'/etc/ufw/user.rules '$ROOT'/etc/ufw/user6.rules 2>/dev/null; cat '$ROOT'/etc/iptables/rules.v* 2>/dev/null; cat '$ROOT'/etc/nftables.conf 2>/dev/null"

# --- Secret inventory (NAMES only — for the rotation checklist) ------------
run secret-inventory  bash -c "find '$ROOT/root' '$ROOT/home' '$ROOT/opt' '$ROOT/srv' '$ROOT/etc' -xdev -type f \\( -name '.env' -o -name '.env.*' -o -name '*.env' \\) ! -name '*.template' ! -name '*.example' 2>/dev/null | while read -r f; do echo \"== \$f (\$(stat -c %y \"\$f\"))\"; grep -oE '^[A-Za-z_][A-Za-z0-9_]*=' \"\$f\" | tr -d '='; done"
run credential-files  bash -c "find '$ROOT/root' '$ROOT/home' '$ROOT/opt' '$ROOT/srv' -xdev -type f \\( -name 'credentials*' -o -name '*.keystore' -o -name 'keystore*' -o -name 'UTC--*' -o -name '*.kdbx' -o -name 'hosts.yml' -o -name '.git-credentials' -o -name '.netrc' -o -name '.pgpass' -o -name '*.json' -path '*solana*' -o -path '*/.claude/.credentials.json' -o -path '*/.config/gh/*' -o -path '*/.aws/*' -o -path '*/.docker/config.json' -o -path '*tailscale*' -name '*.state' \\) -printf '%TY-%Tm-%Td %p\n' 2>/dev/null"

# --- Live-only volatile state ----------------------------------------------
if [[ $LIVE -eq 1 ]]; then
    log WARN "live mode: output below is only as trustworthy as the compromised userland"
    run live-ps       ps auxwwf
    run live-sockets  ss -tupanw
    run live-deleted-exe bash -c "ls -l /proc/*/exe 2>/dev/null | grep '(deleted)'"
    run live-modules  lsmod
    run live-uptime   uptime
    command -v docker >/dev/null && run live-docker docker ps -a --no-trunc
fi

# --- Seal --------------------------------------------------------------------
log INFO "hashing outputs"
( cd "$OUT" && sha256sum ./*.txt > SHA256SUMS ) || log WARN "hashing failed"
TARBALL="$(dirname "$OUT")/triage-$(cat "$ROOT/etc/hostname" 2>/dev/null || echo host)-$STAMP.tar.gz"
tar -czf "$TARBALL" -C "$(dirname "$OUT")" "$(basename "$OUT")" && sha256sum "$TARBALL" | tee -a "$LOG"
log INFO "done. Review first: uid0-accounts, authorized-keys, ld-preload, cron, systemd-units-recent, bin-modified, tmp-files, miner-botnet-iocs, auth-accepted"
log INFO "copy $TARBALL off the rescue system (scp to a clean machine) before reinstalling"
