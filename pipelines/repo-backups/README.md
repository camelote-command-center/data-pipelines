# Repo Backups — disaster recovery

Weekly snapshots of every GitHub repo we care about into Cloudflare R2.
Object Lock (Compliance, 30 days) makes past snapshots immutable so a
compromised credential cannot wipe history.

## What gets backed up

For each repo in `repos.json`:

- Full `git clone --mirror` (all branches, tags, refs)
- Wiki repo if it exists
- Issues, PRs, comments, reviews, releases, tags, branches, labels, milestones
- Workflow definitions, secret names (not values), variables, recent runs
- `RESTORE.md` with rehosting instructions

Packaged as `<repo>-<timestamp>.tar.zst`, uploaded to
`s3://camelote-backups/<org>/<repo>/<year>/W<week>/`.

## Adding a repo

Append to `repos.json`. The workflow matrix picks it up on the next run.

```json
{ "org": "my-org", "repo": "my-new-repo", "is_lovable": false }
```

## Manual run

```
gh workflow run repo_backups.yml
```

Or for a single repo:

```
TARGET_ORG=camelote-command-center \
TARGET_REPO=data-pipelines \
TARGET_IS_LOVABLE=false \
GITHUB_TOKEN=$(gh auth token) \
R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=camelote-backups \
python pipelines/repo-backups/backup.py
```

## Restore

Each archive contains its own `RESTORE.md`. Short version:

```
zstd -d snapshot.tar.zst -o snapshot.tar && tar -xf snapshot.tar
cd <repo>/mirror.git
git push --mirror https://github.com/<new-org>/<new-repo>.git
```
