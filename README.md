# Instagram Followers Exporter (Two-Pass)

This project now supports a safer two-pass workflow:

1. `collect` mode: gather follower usernames + profile links.
2. `enrich` mode: process those usernames one-by-one and fetch full profile fields.

## What full enrich extracts

- Username
- User ID
- Full name
- Bio
- Followers count
- Following count
- Media count
- Private / verified flags
- External URL
- Profile URL

## Important

- Use only on data you are allowed to access.
- Respect Instagram Terms of Use and local laws.
- 429 rate limits can still happen. This script includes slow delays, retries, and checkpoint resume.

## Setup (PowerShell)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
py -m playwright install chromium
```

## Step 1: Collect usernames

```powershell
py main.py target_username --login-user your_username --mode collect --max-followers 5000 --collect-min-delay 2 --collect-max-delay 6 --out followers_export
```

Creates:
- `followers_export_usernames.csv`
- `followers_export_usernames.json`

## Step 2: Enrich full info slowly

```powershell
py main.py target_username --login-user your_username --mode enrich --max-enrich 100 --profile-min-delay 30 --profile-max-delay 60 --retries 6 --out followers_export
```

Creates:
- `followers_export.csv`
- `followers_export.json`
- `followers_export_checkpoint.json`

Run enrich multiple times. The checkpoint file skips already-processed usernames.

## Single command (collect + enrich)

```powershell
py main.py target_username --login-user your_username --mode both --max-followers 500 --max-enrich 100 --out followers_export
```
