# CLAUDE.md

This is an AI-powered inbox management agent for **Trendfeed**, a retention marketing agency. It processes cold email replies from PlusVibe, drafts responses, and pushes them to Slack for human approval before sending.

## Identity — NEVER violate these

- Agency: **Trendfeed** | Site: https://trendfeed.co.uk
- Calendly: https://calendly.com/trendfeed-media/free-email-marketing
- Contact: Abdul.Ibrahim@trendfeed.co.uk | Luzern, Switzerland
- NEVER mention any other agency, agency owner, or their branding. No "Conor", no "Ciaran", no "KMH", no "Kensington Media House". All responses are from Trendfeed only.

## Architecture

```
PlusVibe (reply) → Agent (classify + draft) → Slack (#inbox-review) → Approve/Edit → PlusVibe (send)
```

### Stack
- **PlusVibe** (https://plusvibe.ai/) — cold email platform, source of replies, sends approved responses
- **Slack Bot** — Block Kit interactive messages with Approve / Deny+Edit buttons
- **LLM** — drafts responses, classifies reply types
- **Web scraper** — visits prospect websites to detect brand category
- **Database** — stores templates, feedback/edits, client references

### Key files
- `/src/classifier/` — reply classification logic
- `/src/drafter/` — response drafting with template selection
- `/src/integrations/plusvibe/` — PlusVibe API webhooks and send
- `/src/integrations/slack/` — Slack bot, Block Kit messages, approve/deny handlers
- `/src/scraper/` — prospect website category detection
- `/src/learning/` — feedback loop, edit tracking, few-shot injection
- `/data/templates.json` — reply templates (see Reply Types below)
- `/data/client-portfolio.json` — Trendfeed client references by category

## Core Workflow

1. **Receive** reply via PlusVibe webhook
2. **Classify** the reply into one of the defined reply types
3. **Scrape** prospect's website → detect brand category
4. **Select** matching Trendfeed client references for that category
5. **Draft** response using the appropriate template + prospect context
6. **Post** to Slack with prospect info, original message, draft, and Approve/Deny buttons
7. On **Approve** → send via PlusVibe API from the same inbox/thread
8. On **Deny** → open editable modal in Slack → edited version can be approved and sent
9. **Log** everything: original draft, final sent version, was_edited, diff

## Reply Types & Response Rules

Every response must: use prospect's first name + company name, be warm/confident/non-pushy, end with the Calendly link, and never exceed 150 words.

### Pricing questions
Bespoke pricing, varies by scope. A Growth Consultation call analyses their setup and gives a tailored price structure + roadmap. Trendfeed caps clients at 15–20 for depth of service.

### Services offered
Focus on the transformation (adding significant monthly revenue with no ad spend), not a fixed menu. Usually email & SMS, plus upsells, loyalty, referral programmes. Call to determine exact fit.

### Case studies / proof
Confirm we have case studies. Offer to walk through results on the Growth Consultation call + custom roadmap.

### Send a pitch deck
No pre-defined deck because services are bespoke. Call will provide tailored analysis.

### Referred to another contact
Flag for manual handling in Slack. Draft a warm intro email to the referred person.

### Niche/industry experience — REQUIRES WEBSITE LOOKUP
**Before drafting:** scrape prospect's website, classify their category, then pick 2–3 matching Trendfeed clients. See Client Portfolio below. If no exact match, reference "150+ brands across a dozen industries."

### What makes you different
Boutique, results-driven, scale vertically not horizontally. Max 15–20 clients. Strict vetting. Zero pressure. Money-back guarantee.

### Where did you get my email
Keep it light. "We came across {{companyName}} and saw great potential." Pivot to value.

### Results consistency / failed clients
Honest: can't work for every brand. But results are common because of strict vetting. That's why the guarantee exists.

### Risk / burned before
Empathise. Bringing in the wrong client hurts Trendfeed too. Initial call determines if it's a home-run, then they de-risk it.

### Guarantee / performance-based
Full money-back guarantee. Also open to pure performance on a case-by-case basis. Need to analyse their situation first.

### Already have an agency / doing email
Congrats on existing work. Experience with 150+ partners shows there's always room to grow. Brief chat → free audit doc → useful tips either way.

### Send examples / references
Happy to share after confirming mutual fit on a brief call. Won't bother existing clients for references before that.

### Book a call with us instead
Politely redirect to Calendly. The form collects context + triggers prep automations.

### Don't want to pay / free trial
Empathise but Trendfeed is a business with expenses. After working with 150+ 7 & 8 figure clients, Trendfeed knows what it can deliver.

### Revenue qualification
Below $20K/month → politely decline. $20K–$50K → proceed if potential is visible. Above $50K → standard qualification.

### Auto-reply / OOO
Ignore. Do not draft. Log as "Auto-reply."

### Unsubscribe / removal request
Do NOT draft. Flag in Slack immediately. Process unsubscribe in PlusVibe.

### Hostile / rude reply
Draft a brief polite close-out. Flag for manager.

## Client Portfolio — for niche-matching responses

Reference ONLY clients from https://trendfeed.co.uk:

| Category | Clients |
|---|---|
| Skincare | Frownies, Dermazen |
| Cosmetics/Skincare | Oxygenetix, Happy Head |
| Supplements | KaraMD, NF Sports |
| Food | Walden Farms |
| Pet | Innovet |
| Clothing | Drowsy Sleep Co |
| Body Care | Earthly Body |
| Tech/Accessories | Rolling Square |
| Other | Reference cross-industry experience with 150+ brands |

When prospect is in a category with listed clients, name 2–3 from same or adjacent category. Example: skincare prospect → mention Frownies, Dermazen, and Oxygenetix.

## Slack Message Format

```
📩 NEW REPLY — Requires Review

👤 Prospect:   {{firstName}} {{lastName}}
🏢 Company:    {{companyName}}
🌐 Website:    {{prospectWebsite}}
🏷️ Category:   {{detectedCategory}}
📂 Reply Type: {{classifiedReplyType}}

── Original Message ──
{{prospectReplyText}}

── Drafted Response ──
{{draftedResponse}}
```

Buttons: ✅ Approve | ❌ Deny/Edit

After action: update message to show "✅ Sent by [name] at [time]" or "✏️ Edited & Sent by [name] at [time]"

## Learning / Feedback Loop

Log every interaction:
- `prospect_email`, `prospect_company`, `prospect_category`, `reply_type`
- `ai_draft` vs `final_sent`, `was_edited`, `edit_diff`, `manager`, `timestamp`

Learning rules:
- Include the 5 most recent approved responses per reply type as few-shot examples in LLM prompts
- If a reply type has >40% edit rate, flag for template review
- Weekly: surface top 5 most-edited responses for team review

## Tone Rules

- Warm, confident, conversational — not salesy or corporate
- Short paragraphs, no walls of text
- Max 1–2 exclamation marks per response
- Always personalise with first name + company name
- Always end with value (free audit, roadmap, useful tips)
- Always close with the Calendly link
- Sign off as "Trendfeed Team" or the campaign sender name

## Edge Cases

- Foreign language reply → detect, translate, draft in English, flag for review
- Prospect provides phone number → include in Slack metadata, manager decides
- Uncategorised reply → draft best-effort, flag as "Uncategorised" for manager + potential new template
