"""Repo backup — disaster recovery snapshots for GitHub repos.

Per repo:
  1. git clone --mirror (full bare repo: branches, tags, refs)
  2. Dump issues / PRs / comments / reviews / releases / wiki / actions metadata
     via GitHub REST API (paginated)
  3. tar + zstd into a single archive
  4. Upload to Cloudflare R2 with timestamped key (object lock makes it immutable)

A single snapshot is enough to fully rehost the repo elsewhere if Lovable /
GitHub / etc. become unavailable. See RESTORE.md inside each archive.

Run via GitHub Actions weekly. One repo per matrix job.

Required env:
  GITHUB_TOKEN          PAT with read on Contents, Issues, PRs, Metadata,
                        Actions, Administration (for wiki)
  R2_ACCOUNT_ID         Cloudflare account ID
  R2_ACCESS_KEY_ID      R2 token access key
  R2_SECRET_ACCESS_KEY  R2 token secret
  R2_BUCKET             bucket name (e.g. camelote-backups)
  TARGET_ORG            org to back up (matrix input)
  TARGET_REPO           repo to back up (matrix input)
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.config import Config


GH_API = "https://api.github.com"


def env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        sys.exit(f"missing env {name}")
    return v


def gh_get_paginated(url: str, token: str) -> list:
    """Walk GitHub paginated API. Returns flat list of items."""
    out = []
    page_url = url
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    while page_url:
        r = requests.get(page_url, headers=headers, params={"per_page": 100}, timeout=60)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        out.extend(r.json())
        page_url = r.links.get("next", {}).get("url")
    return out


def run(cmd: list, cwd: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    # Redact tokens embedded in URLs for logs
    safe = [re.sub(r"x-access-token:[^@]+@", "x-access-token:***@", str(a)) for a in cmd]
    print(f"$ {' '.join(safe)}", flush=True)
    res = subprocess.run(cmd, cwd=cwd, check=False, capture_output=True, text=True)
    if res.stdout:
        print(res.stdout, flush=True)
    if res.stderr:
        print(res.stderr, file=sys.stderr, flush=True)
    if check and res.returncode != 0:
        raise subprocess.CalledProcessError(res.returncode, safe)
    return res


def clone_mirror(org: str, repo: str, token: str, dest: Path) -> str:
    """Mirror clone. Returns HEAD commit SHA."""
    url = f"https://x-access-token:{token}@github.com/{org}/{repo}.git"
    run(["git", "clone", "--mirror", url, str(dest)])
    sha = run(["git", "rev-parse", "HEAD"], cwd=str(dest), check=False).stdout.strip()
    return sha or "unknown"


def clone_wiki(org: str, repo: str, token: str, dest: Path) -> bool:
    """Wikis live at <repo>.wiki.git. Returns True if cloned."""
    url = f"https://x-access-token:{token}@github.com/{org}/{repo}.wiki.git"
    res = run(["git", "clone", "--mirror", url, str(dest)], check=False)
    return res.returncode == 0


def dump_github_metadata(org: str, repo: str, token: str, out: Path) -> dict:
    """Dump all reasonable JSON state from the GitHub API."""
    base = f"{GH_API}/repos/{org}/{repo}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    repo_meta = requests.get(base, headers=headers, timeout=60).json()
    (out / "repo.json").write_text(json.dumps(repo_meta, indent=2))

    counts = {}
    for path, key in [
        ("issues?state=all", "issues"),
        ("issues/comments", "issue_comments"),
        ("pulls?state=all", "pulls"),
        ("pulls/comments", "pull_comments"),
        ("releases", "releases"),
        ("tags", "tags"),
        ("branches", "branches"),
        ("labels", "labels"),
        ("milestones?state=all", "milestones"),
        ("actions/workflows", "workflows"),
        ("actions/secrets", "secret_names"),
        ("actions/variables", "variables"),
        ("actions/runs?per_page=50", "recent_runs"),
    ]:
        try:
            data = gh_get_paginated(f"{base}/{path}", token)
            (out / f"{key}.json").write_text(json.dumps(data, indent=2))
            counts[key] = len(data) if isinstance(data, list) else "n/a"
        except requests.HTTPError as e:
            counts[key] = f"error: {e.response.status_code}"

    # PR reviews require per-PR walking
    pulls = json.loads((out / "pulls.json").read_text() or "[]")
    reviews = {}
    for pr in pulls:
        num = pr["number"]
        try:
            r = gh_get_paginated(f"{base}/pulls/{num}/reviews", token)
            if r:
                reviews[str(num)] = r
        except requests.HTTPError:
            pass
    (out / "pull_reviews.json").write_text(json.dumps(reviews, indent=2))
    counts["pull_reviews_prs"] = len(reviews)

    return counts


RESTORE_TEMPLATE = """# RESTORE — {org}/{repo}

Snapshot taken: {when}
HEAD SHA at backup: {sha}
Lovable-managed: {is_lovable}

## What's in this archive

- `mirror.git/` — bare git repo (every branch, tag, ref). The source of truth.
- `wiki.git/` — bare git repo of the GitHub wiki, if one existed.
- `repo.json` — repo settings at backup time (description, default branch, topics, etc.)
- `issues.json` / `issue_comments.json` — every issue + comment
- `pulls.json` / `pull_comments.json` / `pull_reviews.json` — every PR + comments + reviews
- `releases.json` — every release (asset binaries are NOT included; redownload from links if needed)
- `tags.json` / `branches.json` / `labels.json` / `milestones.json`
- `workflows.json` — workflow definitions (the YAML is also in mirror.git)
- `secret_names.json` — names of Actions secrets (values cannot be exported by anyone — recreate manually)
- `variables.json` — Actions variables (NOT secrets)
- `recent_runs.json` — last 50 workflow runs (for context)

## To restore the code

```bash
# 1. Recreate the empty repo on GitHub (or wherever)
gh repo create <new-org>/<new-repo> --private

# 2. Push every ref from the mirror
cd mirror.git
git push --mirror https://github.com/<new-org>/<new-repo>.git
```

## To restore issues / PRs

GitHub's API does not allow recreating issues/PRs with their original numbers
or authors. Two options:

- Use a tool like `gh-importer` or write a small script to recreate issues
  via `POST /repos/{{org}}/{{repo}}/issues` from `issues.json`. Comments and
  PRs require similar walks.
- Treat the JSON dumps as a read-only audit trail and start fresh.

## If this was a Lovable repo ({is_lovable})

The mirror is a complete snapshot of the React/TypeScript frontend at backup
time. To rehost without Lovable:

1. `git clone mirror.git rehosted-frontend`
2. `cd rehosted-frontend && npm install`
3. Deploy to Vercel / Netlify / Cloudflare Pages — it's a standard Vite app.
4. Set the Supabase URL + anon key env vars (from `~/supabase-registry`).

## Secrets

Actions secret VALUES cannot be backed up (write-only by GitHub design).
`secret_names.json` lists what existed so you know what to recreate.
"""


def main():
    org = env("TARGET_ORG")
    repo = env("TARGET_REPO")
    token = env("GITHUB_TOKEN")
    bucket = env("R2_BUCKET")

    is_lovable = os.environ.get("TARGET_IS_LOVABLE", "false").lower() == "true"

    when = datetime.now(timezone.utc)
    iso_year, iso_week, _ = when.isocalendar()
    timestamp = when.strftime("%Y%m%dT%H%M%SZ")

    work = Path(tempfile.mkdtemp(prefix=f"backup-{repo}-"))
    payload_dir = work / repo
    payload_dir.mkdir()

    print(f"=== {org}/{repo} (lovable={is_lovable}) ===", flush=True)
    t0 = time.time()

    sha = clone_mirror(org, repo, token, payload_dir / "mirror.git")
    print(f"  cloned mirror, HEAD={sha}", flush=True)

    if clone_wiki(org, repo, token, payload_dir / "wiki.git"):
        print("  cloned wiki", flush=True)
    else:
        print("  no wiki (or empty)", flush=True)

    counts = dump_github_metadata(org, repo, token, payload_dir)
    print(f"  metadata counts: {counts}", flush=True)

    metadata = {
        "org": org,
        "repo": repo,
        "is_lovable": is_lovable,
        "head_sha": sha,
        "snapshot_at": when.isoformat(),
        "iso_week": f"{iso_year}-W{iso_week:02d}",
        "github_counts": counts,
    }
    (payload_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))
    (payload_dir / "RESTORE.md").write_text(
        RESTORE_TEMPLATE.format(org=org, repo=repo, when=when.isoformat(), sha=sha, is_lovable=is_lovable)
    )

    archive = work / f"{repo}-{timestamp}.tar.zst"
    print(f"  packing {archive.name}...", flush=True)
    # tar | zstd via shell (avoids needing python-zstandard)
    cmd = f"tar -C {work} -cf - {repo} | zstd -19 -T0 -o {archive}"
    subprocess.run(cmd, shell=True, check=True)
    size_mb = archive.stat().st_size / 1024 / 1024
    print(f"  archive size: {size_mb:.1f} MB", flush=True)

    account_id = env("R2_ACCOUNT_ID")
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )

    key = f"{org}/{repo}/{iso_year}/W{iso_week:02d}/{repo}-{timestamp}.tar.zst"
    print(f"  uploading r2://{bucket}/{key}", flush=True)
    s3.upload_file(
        str(archive),
        bucket,
        key,
        ExtraArgs={
            "Metadata": {
                "head-sha": sha,
                "snapshot-at": when.isoformat(),
                "is-lovable": str(is_lovable).lower(),
            }
        },
    )

    elapsed = time.time() - t0
    print(f"=== done in {elapsed:.1f}s, {size_mb:.1f} MB → {key} ===", flush=True)

    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
