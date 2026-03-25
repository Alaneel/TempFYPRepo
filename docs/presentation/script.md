# SingaLiving — Oral Presentation Script
## 20 Minutes, Word-for-Word

**CCDS25-0111 | 08-MAY-2026**
**Total target: 20:00 | Speaking rate: ~140 wpm**

---

### PRE-SHOW CHECKLIST
- [ ] Browser tabs open: `/listings`, a detail page, `/for-you`
- [ ] Backend running: `docker-compose ps` — all green
- [ ] Test query ready: "2-bedroom condo near Orchard under 3M"
- [ ] Extreme query ready: "3-bedroom GCB in Sentosa under 1M"
- [ ] Speaker notes view: press `S` in reveal.js
- [ ] Backup screenshot slides ready (in case system goes down)
- [ ] Cheat sheet printed and on desk

---

## [SLIDE 1 — Title] 0:00–0:15

> Good morning, professors. My name is Wang Yangming. Today I'll present SingaLiving — an AI platform that transforms Singapore's fragmented property market into a unified, intelligent system.

*(click → next slide)*

---

## [SLIDE 2 — The Problem] 0:15–1:30

> Singapore has one of the most expensive property markets in the world. Yet as a buyer today, you face a fragmented landscape.

*(gesture at slide)*

> Three problems motivated this project.

> **First, data fragmentation.** Four competing portals — PropertyGuru, 99.co, SRX, EdgeProp — each with different data schemas, massive cross-platform duplication, and no unified view.

> **Second, methodological limits.** Traditional hedonic price models assume linearity. They can't capture the non-linear pricing dynamics that actually exist in Singapore's market.

> **Third, search usability.** Users are trapped in rigid dropdown filters. You can't say "condo near an international school in Bukit Timah" — there's no field for that.

*(pause, then click to reveal fragment)*

> SingaLiving solves all three. Let me show you how.

**⏱ Check: should be at ~1:30**

---

## [SLIDE 3 — Architecture] 1:30–3:00

> Here's the system architecture. Four layers. Everything in orange is my personal contribution.

*(point at each layer briefly)*

> At the top, the data collection layer. I built the PropertyGuru scraper — which turned out to be 99.7% of all data in the final dataset. My teammates built the other three scrapers.

> In the middle, the entire data pipeline is mine — aggregation, deduplication, ingestion, and the valuation model training pipeline.

> The backend exposes five API endpoints — all the AI ones are mine: semantic search, valuation, chat assistant, and recommendations.

> And the frontend integrates all of these into four pages.

> Let me walk through each contribution, starting with the data layer.

**⏱ Check: should be at ~3:00**

---

## [SLIDE 4a — Data Scale] 3:00–3:45

> The pipeline collected 86,321 raw records across four portals.

*(point at big numbers)*

> After deduplication, we retain 53,497 unique listings — a 62% yield.

> Look at this table. PropertyGuru contributes 53,352 of those — that's 99.7%. SRX had 24,000 raw records but after dedup, only 91 survived — almost everything on SRX was already on PropertyGuru. Same story for 99.co and EdgeProp.

> This is why I prioritised PropertyGuru as the primary scraper.

---

## [SLIDE 4b — Deduplication] 3:45–6:00

> Now, the interesting engineering challenge: deduplication.

> The composite key uses address, bedroom count, bathroom count, and floor area. This gives us 95.5% coverage on the address field.

> But — 4.5% of listings have no address at all.

*(point at ablation table)*

> With a naive address-only key, all of those listings collapse into the same degenerate key — that's **2,410 listings** that become completely indistinguishable. They could be incorrectly merged, or worse, incorrectly discarded.

> My solution: when address is absent, fall back to the listing title. Title has 100% coverage.

> Result? Degenerate key collisions drop from 2,410 to **zero**. Zero false merges.

**⏱ Check: should be at ~6:00**

---

## [SLIDE 5a — Model Design] 6:00–6:45

> Contribution two: AI property valuation.

> Rather than training one universal model, I trained **eight independent XGBoost models** — stratified by property type and transaction mode.

*(gesture at the 8 tags)*

> Why segment? Because HDB flats and luxury GCBs are fundamentally different markets. Prices differ by 10 to 100 times. Pooling them into one model would destroy accuracy for both.

---

## [SLIDE 5b — Performance] 6:45–7:45

> Here are the results.

*(point at HDB rows)*

> HDB models achieve under 9% MAPE — that's a **70% reduction** from the mean baseline, and a 35% improvement over Ridge regression.

> Condo Sale is the strongest: R-squared of 0.945 on a test set of over 16,000 listings.

*(point at Landed/GCB rows)*

> Landed and GCB MAPE is higher — around 24-25%. I'll be upfront about why: these are luxury properties with high heterogeneity and low transaction volume. In future work, I've outlined how adding geospatial features could bring Landed MAPE down to 18-20%.

---

## [SLIDE 5c — SHAP Ablation ⭐] 7:45–10:00

*(slow down — this is the most important slide)*

> Now, here's the finding I consider most academically significant.

*(pause for effect)*

> I ran a feature ablation: what happens when you remove `property_age` from the model?

*(point at HDB row)*

> For HDB Sale, MAPE jumps by **one hundred and thirty percentage points**.

*(PAUSE — full second of silence)*

> The model becomes essentially useless without age.

*(point at Landed row)*

> But for Landed Sale? It barely moves. Just 1.2 percentage points.

*(pause, then explain)*

> Why this dramatic asymmetry?

> It comes down to Singapore's CPF policy. The Central Provident Fund will not provide full financing for HDB flats with less than 60 years remaining on their lease. This creates a **demand cliff** — older HDB flats lose buyers abruptly, and their prices collapse.

> Landed properties, on the other hand, are mostly freehold — permanent tenure. Age barely matters.

> Here's the insight that matters:

*(point at bottom card)*

> I never engineered a CPF-related feature. The model learned this policy-induced price discontinuity **entirely from the data**. Feature ablation didn't just measure accuracy — it validated that the model has captured **real economic structure**. It's not a black box.

**⏱ Check: should be at ~10:00. This is your halfway mark.**

---

## [SLIDE 6a — Semantic Search] 10:00–11:00

> Contribution three: natural language search.

*(point at the example query)*

> Here's a real query: "expat looking for 3-bedroom condo near international school Bukit Timah max 6000."

> On a traditional portal, this is **impossible** to express. There's no dropdown for "international school." There's no field for "expat."

> Claude correctly extracts: districts D10, D11, D21, three bedrooms, condo, rental under six thousand.

*(point at metrics)*

> Evaluated on 47 queries: **96.4% F1**, zero complete failures. To control for self-test bias, a separate blind evaluation — 18 queries by three independent annotators — confirmed **91% adjusted F1**.

---

## [SLIDE 6b — Agentic Enhancements] 11:00–14:00

> But a naive LLM wrapper has a fatal flaw: when it returns zero results, it says nothing. The user is stuck. So I built three agentic enhancements.

*(point at first card)*

> **First, geographic resolution.** "Near Orchard" should search D9, D10, *and* D11 — not just D9. The agent recognises MRT stations and landmarks and auto-expands to adjacent districts.

*(point at second card — spend more time here)*

> **Second — and this is the most impactful — progressive filter relaxation.** If a query returns zero results, the agent relaxes constraints in priority order: **price first**, then property type, then location last. Location is relaxed last because it's typically the hardest constraint for buyers — you won't move from east to west just because it's cheaper.

> This achieved **100% zero-result recovery** across all 12 over-constrained test cases.

*(point at third card)*

> **Third, fallback explanation.** The LLM generates a natural language message explaining exactly what was relaxed and why — all under 800 milliseconds.

> Together, these turn a passive search wrapper into an **intelligent agent** that never leaves users at a dead end. You'll see this in action during the demo.

**⏱ Check: should be at ~14:00**

---

## [SLIDE 7a — Chat Assistant] 14:00–15:30

> Contribution four: conversational AI and recommendations.

> The chat assistant is grounded in property-specific data. Claude receives the full listing attributes, the XGBoost valuation result, and the SHAP importance ranking as context.

> So when a user asks "Is this a fair price?", the answer is backed by **real model outputs**, not hallucinated.

*(point at evaluation numbers)*

> 42 offline interactions across three market segments: **93% factual accuracy, 100% response relevance**. The 7% failures were exclusively SHAP rank order errors — zero price errors.

*(point at bottom card)*

> One finding I think is important: factual accuracy improved from 76% to 93% just by adding safeguard instructions to the system prompt — no model weight changes. Prompt engineering is a **measurable** engineering discipline.

---

## [SLIDE 7b — Recommendations] 15:30–17:00

> The recommendation engine ranks active listings using five-dimensional weighted similarity, computed from the user's saved properties.

*(point at the five tags)*

> Property type, district, price, bedrooms, and — the fifth dimension — **value for money**: the ratio of XGBoost valuation to asking price.

> This is a deliberate design choice. The recommendation engine is **not independent** — it's grounded in the valuation model. It prefers listings that are underpriced relative to model estimates.

*(point at metrics)*

> Offline evaluation on 10 synthetic user profiles: NDCG@5 of **0.811**, and the top-ranked listing achieved **100% match** on both property type and district for every profile.

> For new users with fewer than 3 saves, we fall back to popularity-weighted content similarity — solving the cold-start problem.

**⏱ Check: should be at ~17:00**

---

## [SLIDE 8 — Live Demo] 17:00–19:30

> Let me show you the system live. I'll skip the scrapers — they run for hours — and focus on the AI features.

*(switch to browser, tab 1: /listings)*

> **Demo 1**: Here's the listings page. I'll turn on AI Search and type: "2-bedroom condo near Orchard under 3M."

*(type query, wait for results)*

> Watch the filter tags appear automatically — district D9, D10, D11, two bedrooms, condo, price under 3 million. The LLM extracted all of this.

*(20 sec)*

> **Demo 2**: Now let me intentionally break it.

*(type: "3-bedroom GCB in Sentosa under 1M")*

> This is too specific to return any results. Watch what happens.

*(wait for the fallback)*

> The agent relaxed the price constraint first, then explained what it changed and why. **No dead ends.** This is the agentic enhancement in action.

*(20 sec)*

*(switch to tab 2: /listings/[id])*

> **Demo 3**: On the detail page — here's the AI Valuation panel. Model estimate versus asking price, with a premium or discount badge. Below that, the SHAP attribution — which features pushed the price up or down.

*(scroll to show SHAP)*

*(40 sec)*

> **Demo 4**: I can ask the chat assistant directly.

*(type: "Is this a fair price?")*

> Look — the response cites the SHAP factors directly. It's grounded in the model, not guessing.

*(30 sec)*

*(switch to tab 3: /for-you)*

> **Demo 5**: Finally, the For You page — personalised recommendations based on my saved listings. Each card shows why it was recommended.

*(20 sec)*

**⏱ Check: should be at ~19:30**

*(if system goes down at any point: "Due to a network issue, let me show you the evaluation screenshots instead" — have backup slides ready)*

---

## [SLIDE 9 — Summary] 19:30–20:00

*(switch back to slides)*

> To summarise:

*(gesture at table — don't read every row, just hit the highlights)*

> 53,000 unified listings with zero false merges. HDB valuation under 9% MAPE — 70% below baseline. Semantic search at 96% F1. Three agentic enhancements with 100% zero-result recovery. Chat assistant at 93% factual accuracy. And a recommendation engine with NDCG@5 of 0.811.

*(click to reveal closing)*

> SingaLiving transforms four fragmented portals into one intelligent platform — from raw HTML to personalised, AI-powered real estate insights.

*(click to reveal "Thank you")*

> Thank you.

*(stop talking. smile. wait for questions.)*

**⏱ Target: 20:00 exactly**

---

## POST-PRESENTATION: Q&A Reminders

1. **Listen to the full question** before answering
2. **"That's a great question"** = 2 seconds of thinking time
3. **Numbers**: say "approximately" if unsure — never make up a figure
4. **Weaknesses**: be honest, then pivot to future work with specific numbers
5. **Contribution boundary**: PropertyGuru scraper (99.7%), all AI, all pipeline = mine. Other 3 scrapers = teammates.
6. **Most likely questions** (in order):
   - Your vs teammate contributions?
   - Why is Landed/GCB MAPE so high?
   - XGBoost vs neural nets?
   - F1 96.4% — self-evaluated bias?
   - Listing price vs transacted price?
