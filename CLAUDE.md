# Agent Instructions

You're working inside the **WAT framework** (Workflows, Agents, Tools). Probabilistic AI handles reasoning; deterministic code handles execution.

## The WAT Architecture

**Layer 1: Workflows (The Instructions)** — Markdown SOPs in `workflows/`. Objective, required inputs, tools, expected outputs, edge cases.

**Layer 2: Agents (The Decision-Maker)** — This is your role. Read the relevant workflow, run tools in order, handle failures, ask when unclear.

**Layer 3: Tools (The Execution)** — Python scripts in `tools/` and `src/`. API calls, transformations, file ops, DB queries. Credentials in `.env`.

**Why it matters:** if every step is 90% accurate, five steps compound to 59%. Offloading execution to deterministic scripts keeps orchestration reliable.

## How to Operate

**1. Look for existing tools first.** Check `tools/` and `src/integrations/` before writing new scripts.

**2. Learn and adapt when things fail.** Read the full trace. Fix the script. If it uses paid APIs, ask before retrying. Document lessons in the workflow.

**3. Keep workflows current.** Evolve them as you learn. Don't overwrite without asking unless told to.

## File Structure

```
.tmp/               # Temporary files (regenerable)
data/               # Static reference data (canonical facts, spec modules)
src/                # Application code
  classifier.py     # 32-intent reply classifier
  drafter.py        # Reply drafter (canonical facts + voice)
  integrations/
    plusvibe.py     # Unibox + send + lead data
    slack.py        # Review card + disregard + escalation posts
    trendtrack.py   # Brand resolution: monthly visits, ads, country
    beehiiv.py      # Auto-subscribe positive-reply leads
    calendly.py     # (dormant — webhook verifier, not wired)
tools/              # Standalone deterministic scripts
workflows/          # Markdown SOPs
main.py             # FastAPI entrypoint (Railway)
reply_agent_instructions.md   # Source-of-truth spec for the reply agent
.env                # Secrets (never commit)
```

---

## Live System — Inbox Reply Agent

**Stack:** FastAPI + uvicorn on Railway · Claude claude-sonnet-4-6 · Slack Block Kit · Trendtrack public API · Beehiiv

**Ground truth for behaviour:** [reply_agent_instructions.md](reply_agent_instructions.md). Any drift between code and that file — the file wins, and code is wrong.

**Ground truth for facts + intent library:** [data/reply_agent_spec.py](data/reply_agent_spec.py). Both classifier and drafter import from here — do not duplicate facts elsewhere.

### Active campaign
| Campaign | ID | Slack channel |
|---|---|---|
| 2 weeks – May [Outlook] | `69fb3fa29465cdb03f8c811f` | `#inbox-agent-reply` (`C0AJG9V9JSE`) |

Update `CAMPAIGN_ACTIVE` in `main.py` when swapping.

### Flow
```
PlusVibe unibox / webhook  →  _process_reply
  1. Beehiiv subscribe (positive-reply signal)
  2. Trendtrack resolve → intel block (visits, ads, country)
  3. Classify intent (1..32) with thread context
  4. Resolve conditional intent 24; apply geography gate
  5. Route by disposition:
       draft     → drafter → Slack review card
       disregard → Slack disregard notification (no send)
       escalate  → Slack "needs a human"
       park      → silent (OOO / wait-for-date)
       stop      → silent, no reply
       suppress  → Slack unsubscribe alert
```

Follow-ups, Google Sheets CRM, weekly/monthly reports, and @mention CRM commands have been removed from this build; they'll be rebuilt against the new spec later.

### Env vars (Railway)
Required: `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID`, `SLACK_SIGNING_SECRET`, `PLUSVIBE_API_KEY`, `PLUSVIBE_WORKSPACE_ID`, `ANTHROPIC_API_KEY`, `BEEHIIV_API_KEY`, `BEEHIIV_PUBLICATION_ID`, `TRENDTRACK_API_KEY`.

Optional / kept for the next CRM build: `GOOGLE_SHEETS_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON`, `CALENDLY_WEBHOOK_SECRET`, `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`.

### Webhooks to register
- PlusVibe: `POST /webhook/plusvibe`
- Slack actions: `POST /webhook/slack/actions`
- Slack events: `POST /webhook/slack/events` (URL verification only in this build)

### Admin endpoints (diagnostic)
- `GET  /health`
- `GET  /admin/workspaces`
- `POST /admin/register-webhook`      — body: `{"url": "https://..."}`
- `POST /admin/retry-beehiiv`
- `POST /admin/test-slack`
- `GET  /admin/trendtrack-lookup?q=<domain or brand>`   — resolve one brand end-to-end
- `POST /admin/reprocess-last-webhook` — body: raw PlusVibe webhook JSON to replay

### Voice + facts rules (never break)
- No dashes of any kind in drafts (hyphen, en, em — all rewritten).
- Only figures/claims in `CANONICAL_FACTS` are ever stated. "150+ brands" and "$100M+" are BOTH wrong.
- Never quote the intel block back to the prospect.
- Never claim to be human if asked directly (intent 21 escalates).
- Own Trendfeed's cold emails — never disclaim or distance from them.
