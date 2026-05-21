#!/usr/bin/env bash
# ============================================================================
# vd_enrichment — VPS deployment script
# ============================================================================
# Idempotent. Re-runnable. Takes:
#   $1  — VPS host (one of: vps1, vps2, vps3) OR explicit IP
#   $2  — parser list file (one parser dir name per line; comments # ok)
#
# What it does:
#   1. SSHs to the VPS, installs minimal apt packages
#   2. Clones (or pulls) data-pipelines repo at /opt/lamap/data-pipelines
#   3. Sets up per-parser venv at /opt/lamap/parsers/<dataset>/venv
#   4. Writes /etc/cron.d/lamap-vd-enrichment with the assigned schedule
#   5. Writes /etc/logrotate.d/lamap
#   6. For each parser: --dry-run --limit 10  (no DB writes)
#
# Safety:
#   - Does NOT install if /opt/lamap/.deploy.lock exists (manual unlock required)
#   - Does NOT write secrets — env vars must be in /opt/lamap/.env (chmod 600,
#     pre-populated manually with SUPABASE_DB_URI etc.)
#   - Does NOT enable cron on first run; final step is `systemctl reload cron`
#     which the operator runs after reviewing /etc/cron.d output
# ============================================================================
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <vps1|vps2|vps3|<ip>> <parser-list-file>"
  echo "Example: $0 vps1 ./vps1.parsers.txt"
  exit 2
fi

VPS_ALIAS="$1"
PARSER_LIST="$2"

case "$VPS_ALIAS" in
  vps1) IP="145.223.82.190" ;;
  vps2) IP="31.97.122.135" ;;
  vps3) IP="46.202.153.114" ;;
  *)    IP="$VPS_ALIAS" ;;
esac

if [[ ! -f "$PARSER_LIST" ]]; then
  echo "ERROR: parser list file not found: $PARSER_LIST" >&2
  exit 2
fi

# Cron schedules per host
case "$VPS_ALIAS" in
  vps1) CRON_MIN=0  ; CRON_HOUR=4 ;;     # 04:00 UTC monthly
  vps2) CRON_MIN=30 ; CRON_HOUR=4 ;;     # 04:30 UTC monthly
  vps3) CRON_MIN=0  ; CRON_HOUR=5 ;;     # 05:00 UTC monthly
  *)    CRON_MIN=0  ; CRON_HOUR=4 ;;
esac

REPO_URL="https://github.com/camelote-command-center/data-pipelines.git"
BRANCH="feature/vd-enrichment"
PARSER_LIST_REMOTE="/opt/lamap/parsers.list"

# ----- Step 1: apt + repo -----
ssh -o StrictHostKeyChecking=accept-new "root@${IP}" bash -s <<EOSH
set -euo pipefail
mkdir -p /opt/lamap /var/log/lamap

if [[ -f /opt/lamap/.deploy.lock ]]; then
  echo "DEPLOY LOCK PRESENT — aborting. Remove /opt/lamap/.deploy.lock to proceed."
  exit 3
fi

# Minimal package set
DEBIAN_FRONTEND=noninteractive apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 python3-pip python3-venv git curl jq

# Clone or update repo
if [[ ! -d /opt/lamap/data-pipelines ]]; then
  git clone --branch "${BRANCH}" "${REPO_URL}" /opt/lamap/data-pipelines
else
  cd /opt/lamap/data-pipelines
  git fetch --quiet origin "${BRANCH}"
  git checkout --quiet "${BRANCH}"
  git pull --quiet --ff-only
fi

# .env stub (NEVER overwrite existing)
if [[ ! -f /opt/lamap/.env ]]; then
  cat > /opt/lamap/.env <<'ENV'
# Set the session_pooler_uri from ~/supabase-registry/supabase-projects.json -> 're-llm' entry.
# DO NOT use db.<ref>.supabase.co — IPv6-only, will fail.
SUPABASE_DB_URI=
PARSER_HOST=vps-PLACEHOLDER_IP
ENV
  chmod 600 /opt/lamap/.env
  echo "Created /opt/lamap/.env stub — POPULATE SUPABASE_DB_URI before enabling cron."
fi

# logrotate
cat > /etc/logrotate.d/lamap <<'ROTATE'
/var/log/lamap/*.log {
    weekly
    rotate 8
    compress
    missingok
    notifempty
    sharedscripts
    copytruncate
}
ROTATE

echo "Step 1 done: repo + base setup OK."
EOSH

# ----- Step 2: copy parser list + per-parser venvs + dry-run -----
scp "$PARSER_LIST" "root@${IP}:${PARSER_LIST_REMOTE}"
ssh "root@${IP}" bash -s <<EOSH
set -euo pipefail
cd /opt/lamap/data-pipelines

PARSERS=\$(grep -v '^#' /opt/lamap/parsers.list | grep -v '^$')

# Per-parser venv
for p in \$PARSERS; do
  echo "--- Setting up venv for \$p ---"
  PDIR="/opt/lamap/parsers/\$p"
  mkdir -p "\$PDIR"
  if [[ ! -d "\$PDIR/venv" ]]; then
    python3 -m venv "\$PDIR/venv"
  fi
  "\$PDIR/venv/bin/pip" install --quiet --upgrade pip
  "\$PDIR/venv/bin/pip" install --quiet psycopg2-binary
done

# Dry-run validation
echo ""
echo "=========== DRY-RUN VALIDATION ==========="
for p in \$PARSERS; do
  echo "--- DRY RUN: \$p ---"
  PDIR="/opt/lamap/parsers/\$p"
  set +e
  PYTHONPATH=/opt/lamap/data-pipelines "\$PDIR/venv/bin/python" -m \
    "pipelines.vd_enrichment.\$p.run" --dry-run --limit 5 \
    2>&1 | tee "/var/log/lamap/\${p}.dry.log" | tail -3
  RC=\$?
  set -e
  if [[ \$RC -ne 0 ]]; then
    echo "DRY RUN FAILED for \$p — see /var/log/lamap/\${p}.dry.log"
  fi
done
echo ""

# Cron file
CRONFILE="/etc/cron.d/lamap-vd-enrichment"
cat > "\$CRONFILE" <<CRONHDR
# vd_enrichment parsers — managed by deploy.sh — DO NOT EDIT BY HAND
# Each line runs monthly (1st of month) at the assigned time.
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
MAILTO=""

CRONHDR
for p in \$PARSERS; do
  echo "${CRON_MIN} ${CRON_HOUR} 1 * * root cd /opt/lamap/data-pipelines && set -a && . /opt/lamap/.env && set +a && PYTHONPATH=/opt/lamap/data-pipelines /opt/lamap/parsers/\$p/venv/bin/python -m pipelines.vd_enrichment.\$p.run >> /var/log/lamap/\$p.log 2>&1" >> "\$CRONFILE"
done

# Federal BAV transit: ALSO add a quarterly line if the parser appears in the list
if grep -q '^federal_bav_transit$' /opt/lamap/parsers.list; then
  echo "30 5 1 1,4,7,10 * root cd /opt/lamap/data-pipelines && set -a && . /opt/lamap/.env && set +a && PYTHONPATH=/opt/lamap/data-pipelines /opt/lamap/parsers/federal_bav_transit/venv/bin/python -m pipelines.vd_enrichment.federal_bav_transit.run >> /var/log/lamap/federal_bav_transit.log 2>&1" >> "\$CRONFILE"
fi

chmod 644 "\$CRONFILE"
echo ""
echo "Cron written to \$CRONFILE. Review before reloading cron:"
echo "  systemctl reload cron"
echo ""
echo "Deploy complete (cron NOT yet reloaded — operator step)."
EOSH

echo ""
echo "================================================================"
echo "Deploy of $VPS_ALIAS ($IP) finished."
echo "Next manual steps:"
echo "  1. SSH and populate /opt/lamap/.env with SUPABASE_DB_URI"
echo "  2. Review /etc/cron.d/lamap-vd-enrichment"
echo "  3. ssh root@${IP} 'systemctl reload cron'"
echo "================================================================"
