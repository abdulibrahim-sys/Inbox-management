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

**Stack:** FastAPI + uvicorn on Railway · Claude claude-sonnet-4-6 · Slack Block Kit · Beehiiv

**Ground truth for behaviour:** [reply_agent_instructions.md](reply_agent_instructions.md). Any drift between code and that file — the file wins, and code is wrong.

**Ground truth for facts + intent library:** [data/reply_agent_spec.py](data/reply_agent_spec.py). Both classifier and drafter import from here — do not duplicate facts elsewhere.

### Active campaigns
Both feed replies into `#inbox-agent-reply` (`C0AJG9V9JSE`). Update the `ACTIVE_CAMPAIGNS` list in `main.py` when campaigns start/pause in PlusVibe.

| Campaign | ID |
|---|---|
| Ai-ark-big brands - Copy | `6a4bb4325d0a8ff67b02b811` |
| 2 weeks - july           | `6a60f7d25756c23899f6bbd2` |

### Flow
```
PlusVibe unibox / webhook  →  _process_reply
  1. Beehiiv subscribe (positive-reply signal)
  2. Classify intent (1..32) with thread context
  3. TLD geo gate (.in / .pk → stop); intent 24 always escalates
  4. Route by disposition:
       draft     → drafter → Slack review card
       disregard → Slack disregard notification (no send)
       escalate  → Slack "needs a human"
       park      → silent (OOO / wait-for-date)
       stop      → silent, no reply
       suppress  → Slack unsubscribe alert
```

Follow-up engine (Phase 1 — backlog reactivation): `POST /admin/backlog/scan`
then `POST /admin/backlog/next-batch` (5 at a time). Drafts personalise from
thread context only — no brand research.

Follow-ups, Google Sheets CRM, weekly/monthly reports, and @mention CRM commands have been removed from this build; they'll be rebuilt against the new spec later.

### Env vars (Railway)
Required: `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID`, `SLACK_SIGNING_SECRET`, `PLUSVIBE_API_KEY`, `PLUSVIBE_WORKSPACE_ID`, `ANTHROPIC_API_KEY`, `BEEHIIV_API_KEY`, `BEEHIIV_PUBLICATION_ID`, `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`.

Optional / kept for the next CRM build: `GOOGLE_SHEETS_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON`, `CALENDLY_WEBHOOK_SECRET`.

`TRENDTRACK_API_KEY` was removed — the Trendtrack integration was ripped out on 2026-07-28 (too rate-limited, too much complexity). Reply-agent intel block and follow-up ad tie-ins were replaced with simpler alternatives (TLD geo check, thread-only follow-ups).

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
- `POST /admin/reprocess-last-webhook` — body: raw PlusVibe webhook JSON to replay
- `POST /admin/backlog/scan`           — enumerate ghosted leads for reactivation
- `GET  /admin/backlog/queue`          — peek at what's queued
- `POST /admin/backlog/next-batch`     — draft + post the next 5 follow-ups

### Voice + facts rules (never break)
- No dashes of any kind in drafts (hyphen, en, em — all rewritten).
- Only figures/claims in `CANONICAL_FACTS` are ever stated. "150+ brands" and "$100M+" are BOTH wrong.
- Never claim to be human if asked directly (intent 21 escalates).
- Own Trendfeed's cold emails — never disclaim or distance from them.
