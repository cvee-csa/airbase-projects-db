# airbase-projects-db

Automated sync between the **Zendesk IT-Operations-Projects** ticket queue and the **CatherineAirbase** Ticket Log in Airtable.

## What it does

A GitHub Actions workflow runs every weekday at 8:00 AM Pacific and:

1. Pulls all existing records from the Airtable Ticket Log
2. Fetches tickets from the Zendesk IT-Operations-Projects group
3. Creates Airtable records for any new tickets
4. Updates status, assignee, and resolved date for changed tickets

You can also trigger a sync manually from the **Actions** tab at any time.

## Setup

### 1. Add repository secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret               | Value                                                                |
| -------------------- | -------------------------------------------------------------------- |
| `ZENDESK_SUBDOMAIN`  | `cloudsecurityalliance`                                              |
| `ZENDESK_EMAIL`      | Your Zendesk agent email (e.g. `cvee@cloudsecurityalliance.org`)     |
| `ZENDESK_API_TOKEN`  | Zendesk API token — **Admin → Channels → API**                      |
| `AIRTABLE_PAT`       | Airtable Personal Access Token with `data.records:read` and `data.records:write` scopes on the CatherineAirbase base — [create one here](https://airtable.com/create/tokens) |

### 2. Push and enable

Once the secrets are saved, push this repo. The workflow will run on the next weekday morning, or you can trigger it immediately via **Actions → Zendesk → Airtable Sync → Run workflow**.

## Running locally

```bash
export ZENDESK_SUBDOMAIN="cloudsecurityalliance"
export ZENDESK_EMAIL="your-email@cloudsecurityalliance.org"
export ZENDESK_API_TOKEN="your-token-here"
export AIRTABLE_PAT="your-pat-here"

pip install requests
python sync.py
```

## Customizing the schedule

Edit the cron in `.github/workflows/sync.yml`. The time is UTC (GitHub Actions requirement). Default `0 15 * * 1-5` = 3 PM UTC = 8 AM PDT.

Examples:
- Hourly on weekdays: `0 * * * 1-5`
- Twice daily (8 AM + 4 PM Pacific): `0 15,23 * * 1-5`
- Every day including weekends: `0 15 * * *`
