# Agent Instructions

You're working inside the **WAT framework** (Workflows, Agents, Tools). This architecture separates concerns so that probabilistic AI handles reasoning while deterministic code handles execution. That separation is what makes this system reliable.

## The WAT Architecture

**Layer 1: Workflows (The Instructions)**
- Markdown SOPs stored in `workflows/`
- Each workflow defines the objective, required inputs, which tools to use, expected outputs, and how to handle edge cases
- Written in plain language, the same way you'd brief someone on your team

**Layer 2: Agents (The Decision-Maker)**
- This is your role. You're responsible for intelligent coordination.
- Read the relevant workflow, run tools in the correct sequence, handle failures gracefully, and ask clarifying questions when needed
- You connect intent to execution without trying to do everything yourself
- Example: If you need to pull data from a website, don't attempt it directly. Read `workflows/scrape_website.md`, figure out the required inputs, then execute `tools/scrape_single_site.py`

**Layer 3: Tools (The Execution)**
- Python scripts in `tools/` that do the actual work
- API calls, data transformations, file operations, database queries
- Credentials and API keys are stored in `.env`
- These scripts are consistent, testable, and fast

**Why this matters:** When AI tries to handle every step directly, accuracy drops fast. If each step is 90% accurate, you're down to 59% success after just five steps. By offloading execution to deterministic scripts, you stay focused on orchestration and decision-making where you excel.

## How to Operate

**1. Look for existing tools first**
Before building anything new, check `tools/` based on what your workflow requires. Only create new scripts when nothing exists for that task.

**2. Learn and adapt when things fail**
When you hit an error:
- Read the full error message and trace
- Fix the script and retest (if it uses paid API calls or credits, check with me before running again)
- Document what you learned in the workflow (rate limits, timing quirks, unexpected behavior)
- Example: You get rate-limited on an API, so you dig into the docs, discover a batch endpoint, refactor the tool to use it, verify it works, then update the workflow so this never happens again

**3. Keep workflows current**
Workflows should evolve as you learn. When you find better methods, discover constraints, or encounter recurring issues, update the workflow. That said, don't create or overwrite workflows without asking unless I explicitly tell you to. These are your instructions and need to be preserved and refined, not tossed after one use.

## The Self-Improvement Loop

Every failure is a chance to make the system stronger:
1. Identify what broke
2. Fix the tool
3. Verify the fix works
4. Update the workflow with the new approach
5. Move on with a more robust system

This loop is how the framework improves over time.

## File Structure

**What goes where:**
- **Deliverables**: Final outputs go to cloud services (Google Sheets, Slides, etc.) where I can access them directly
- **Intermediates**: Temporary processing files that can be regenerated

**Directory layout:**
```
.tmp/           # Temporary files (scraped data, intermediate exports). Regenerated as needed.
tools/          # Python scripts for deterministic execution
workflows/      # Markdown SOPs defining what to do and how
.env            # API keys and environment variables (NEVER store secrets anywhere else)
credentials.json, token.json  # Google OAuth (gitignored)
```

**Core principle:** Local files are just for processing. Anything I need to see or use lives in cloud services. Everything in `.tmp/` is disposable.

## Bottom Line

You sit between what I want (workflows) and what actually gets done (tools). Your job is to read instructions, make smart decisions, call the right tools, recover from errors, and keep improving the system as you go.

Stay pragmatic. Stay reliable. Keep learning.

---

## Live System — Inbox Management Agent

**Stack:** FastAPI + uvicorn on Railway, Claude claude-sonnet-4-6, Upstash Redis, Slack Block Kit, Google Sheets API v4

### Campaigns
| Campaign | Slack Channel | Classifier | Drafter |
|---|---|---|---|
| 2 Weeks (Trendfeed) | `#inbox-agent-reply` (`C0AJG9V9JSE`) | `src/classifier.py` | `src/drafter.py` |
| AI Search Visibility | `#ai-visibility-replies` (`C0AP56KJKV2`) | `src/ai_visibility_classifier.py` | `src/ai_visibility_drafter.py` |

### Reply Agent Rules
- Only handle `LEAD_MARKED_AS_INTERESTED` webhook events
- Route by `campaign_name` field (lowercase substring match against `AI_VISIBILITY_CAMPAIGNS`)
- First reply: no case studies; follow-ups may include them
- Responses: 60–100 words, handle objection first, pivot to call with "You can grab a time here 👉 [link]"
- Own Trendfeed's cold emails — never disclaim or distance from them
- Client count: 150+ across a dozen industries

### CRM — Google Sheets
Tabs: `Pipeline`, `Call Log`, `Follow-Up Schedule`, `Monthly Metrics`
Protected (read-only): `Dashboard`, `Loss Analysis`

Data start rows: Pipeline=3, Call Log=3, Follow-Up Schedule=4, Monthly Metrics=4

**Auto-writes:**
- `POST /webhook/plusvibe` → appends/updates Pipeline row
- `POST /webhook/calendly` (invitee.created) → appends Call Log row + updates Pipeline stage to "Call Booked"
- `POST /webhook/calendly` (invitee.canceled) → marks no-show, schedules nurture
- Approved reply → updates Pipeline `last_touch`, `stage`, `next_followup`

**Slack CRM bot (@mention → `/webhook/slack/events`):**
- `@bot [name] showed / no-showed` — updates Call Log show status; no-show triggers auto-nurture schedule
- `@bot close [name] at $X` — marks Closed Won, logs revenue, updates Monthly Metrics
- `@bot lose [name] reason [text]` — marks Closed Lost, logs reason
- `@bot proposal sent to [name]` — advances stage
- `@bot nurture [name]` — moves to long-term nurture cadence
- `@bot note [name]: [text]` — appends note
- `@bot reschedule [name] to [date]` — updates Call Log
- `@bot book follow-up [name] for [date]` — sets next_followup in Pipeline
- `@bot stats [this week|this month]` — returns pipeline snapshot

**Nurture cadences (Follow-Up Schedule days):**
- No Show: 1, 3, 7, 14, 30, 60, 90
- Closed Lost – timing: 30, 60, 90, 180
- Closed Lost – budget: 60, 90, 180
- Closed Lost – decided: 90, 180
- Ghosted: 7, 14, 30, 60

**Scheduled reports:**
- Weekly: Friday 7pm EST → Slack
- Monthly: last day of month 7pm EST → Slack

### Beehiiv Newsletter
- All `LEAD_MARKED_AS_INTERESTED` events auto-subscribe to Beehiiv
- Failed subscriptions queued in Redis (`beehiiv:retry_queue`) and retried every 24h
- Pub ID must have `pub_` prefix (auto-prefixed if missing)

### Env Vars Required (Railway)
`SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID`, `SLACK_CHANNEL_AI_VISIBILITY`, `SLACK_SIGNING_SECRET`,
`PLUSVIBE_API_KEY`, `PLUSVIBE_WORKSPACE_ID`, `ANTHROPIC_API_KEY`,
`UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`,
`BEEHIIV_API_KEY`, `BEEHIIV_PUBLICATION_ID`,
`GOOGLE_SERVICE_ACCOUNT_JSON` (raw JSON or base64), `GOOGLE_SHEETS_ID`,
`CALENDLY_WEBHOOK_SECRET`

### Webhooks to Register
- PlusVibe: `POST /webhook/plusvibe`
- Calendly: `POST /webhook/calendly`
- Slack Actions: `POST /webhook/slack/actions`
- Slack Events: `POST /webhook/slack/events` (subscribe to `app_mention`)
