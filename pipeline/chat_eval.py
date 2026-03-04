"""
Chat Assistant Quality Evaluation
==================================
Evaluates the chat assistant across 42 representative interactions spanning
5 question categories across 3 property segments (Condo Sale, HDB Rent,
Landed Sale). Uses the same system-prompt construction as the live chat
endpoint (listings.py), but runs offline without a DB connection.

Scoring rubric (binary per interaction):
  factual_consistency:  Does the response contradict the XGBoost estimate or SHAP output?
                        0 = contradiction found, 1 = consistent
  relevance:            Does the response address the question asked?
                        0 = off-topic / refused / empty, 1 = addresses question

Output:
  pipeline/chat_eval_results.json
  Console table + per-category summary

Usage:
    python pipeline/chat_eval.py

Requires:
    ANTHROPIC_API_KEY in environment (or backend/.env)
"""

import json
import os
import sys
import time
from pathlib import Path

# ── load .env if present ──────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / "backend" / ".env")
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import anthropic

PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_PATH  = PROJECT_ROOT / "pipeline" / "chat_eval_results.json"

# ── Shared listing + valuation context (representative Condo Sale listing) ────
LISTING_CONTEXT_CONDO = """Property: Riverside Suites at Robertson Quay
Type: Condominium
Transaction: property-for-sale
Listed price: S$2.00M
Bedrooms: 2
Bathrooms: 2
Floor area: 980 sqft
Tenure: Freehold
District: 9
Built year: 2005
Address: Robertson Quay, Singapore 238236"""

VALUATION_CONTEXT_CONDO = """=== AI Valuation Result ===
Estimated value: S$1.71M
Range: S$1.48M – S$1.94M
Model MAPE: ±13.4%
Verdict vs listed price: Overpriced (+17.0%)
SHAP price attribution factors:
  • Floor area (980 sqft): −S$142K
  • Property age (21 yr): −S$98K
  • District 9 location: +S$187K
  • Freehold tenure: +S$63K
  • Bedroom count (2): −S$31K"""

# ── HDB Rent listing context ──────────────────────────────────────────────────
LISTING_CONTEXT_HDB = """Property: HDB Flat at Toa Payoh
Type: HDB
Transaction: property-for-rent
Listed price: S$3,200/month
Bedrooms: 4
Bathrooms: 2
Floor area: 1,100 sqft
Tenure: 99-year leasehold
District: 12
Built year: 1988
Address: Toa Payoh, Singapore 310150"""

VALUATION_CONTEXT_HDB = """=== AI Valuation Result ===
Estimated value: S$2,880/month
Range: S$2,624/month – S$3,136/month
Model MAPE: ±8.9%
Verdict vs listed price: Overpriced (+11.1%)
SHAP price attribution factors:
  • Bedroom count (4): +S$420/month
  • Floor area (1,100 sqft): +S$310/month
  • District 12 location: −S$180/month
  • Property age (38 yr): −S$290/month
  • Leasehold tenure: −S$95/month"""

# ── Landed Sale listing context ───────────────────────────────────────────────
LISTING_CONTEXT_LANDED = """Property: Semi-Detached House at Bukit Timah
Type: Landed
Transaction: property-for-sale
Listed price: S$4.80M
Bedrooms: 5
Bathrooms: 4
Floor area: 3,200 sqft
Tenure: Freehold
District: 21
Built year: 1995
Address: Bukit Timah, Singapore 589638"""

VALUATION_CONTEXT_LANDED = """=== AI Valuation Result ===
Estimated value: S$4.15M
Range: S$3.06M – S$5.24M
Model MAPE: ±26.3%
Verdict vs listed price: Overpriced (+15.7%)
SHAP price attribution factors:
  • Floor area (3,200 sqft): +S$820K
  • Freehold tenure: +S$410K
  • District 21 location: +S$290K
  • Property age (31 yr): −S$380K
  • Bedroom count (5): +S$210K"""


def make_system_prompt(listing_ctx: str, valuation_ctx: str) -> str:
    return (
        "You are a knowledgeable Singapore real estate advisor. "
        "Answer the user's questions about the property below concisely and helpfully. "
        "Use the valuation data when relevant. Be honest about uncertainty. "
        "Keep replies under 120 words unless asked for more detail. "
        "Always cite specific numbers from the data when available. "
        "IMPORTANT: Reply in plain text only. Do NOT use Markdown formatting — no bold (**text**), "
        "no bullet points, no headers. Write naturally as if texting a friend.\n\n"
        "CRITICAL UNCERTAINTY RULE: Whenever you mention a MAPE figure or model accuracy, "
        "you MUST immediately follow it with an explicit uncertainty caveat such as "
        "'so the true value could differ significantly' or 'treat this as a rough guide, not a precise figure'. "
        "Never present the AI estimate or MAPE as definitive — always frame it as an estimate with meaningful uncertainty.\n\n"
        "=== Listing Context ===\n"
        + listing_ctx + "\n\n"
        + valuation_ctx
    )


SYSTEM_PROMPT = make_system_prompt(LISTING_CONTEXT_CONDO, VALUATION_CONTEXT_CONDO)

# ── Test cases ────────────────────────────────────────────────────────────────
# Each case: (category, question, grounding_checks, relevance_keywords)
#   grounding_checks: strings that must NOT appear contradicting the data
#   relevance_keywords: at least one must appear for relevance=1
TEST_CASES_CONDO = [
    # ── Category 1: Valuation verdict (5 cases) ───────────────────────────────
    {
        "category": "Valuation verdict",
        "question": "Is this condo fairly priced?",
        "must_mention_any": ["overpriced", "overprice", "above", "17", "premium", "high"],
        "must_not_contradict": ["fair value", "below estimate", "good deal", "underpriced"],
    },
    {
        "category": "Valuation verdict",
        "question": "How does the listed price compare to the AI estimate?",
        "must_mention_any": ["1.71", "2.0", "2 million", "17%", "overpriced", "above"],
        "must_not_contradict": ["below estimate", "undervalued", "1.5", "1.3"],
    },
    {
        "category": "Valuation verdict",
        "question": "Should I negotiate the price down?",
        "must_mention_any": ["overpriced", "estimate", "1.71", "negotiate", "17"],
        "must_not_contradict": ["fair price", "don't negotiate", "no room"],
    },
    {
        "category": "Valuation verdict",
        "question": "What is the fair value range for this property?",
        "must_mention_any": ["1.48", "1.94", "range", "1.71"],
        "must_not_contradict": ["2 million", "2.0M is fair", "no range"],
    },
    {
        "category": "Valuation verdict",
        "question": "Is the S$2M asking price reasonable for Robertson Quay?",
        "must_mention_any": ["overpriced", "estimate", "1.71", "17%", "above"],
        "must_not_contradict": ["reasonable", "fair", "good price", "worth 2"],
    },

    # ── Category 2: SHAP factor attribution (4 cases) ─────────────────────────
    {
        "category": "SHAP factor attribution",
        "question": "What factors are dragging down the valuation?",
        "must_mention_any": ["age", "floor area", "980", "21 year", "2005", "sqft"],
        "must_not_contradict": ["tenure dragging", "district negative", "bedroom positive"],
    },
    {
        "category": "SHAP factor attribution",
        "question": "Why is the valuation lower than the asking price?",
        "must_mention_any": ["age", "floor area", "142", "98", "overpriced", "17"],
        "must_not_contradict": ["district is negative", "freehold is negative"],
    },
    {
        "category": "SHAP factor attribution",
        "question": "What is the biggest positive factor in the valuation?",
        "must_mention_any": ["district", "district 9", "location", "187"],
        "must_not_contradict": ["freehold is the biggest", "bedroom is the biggest", "floor area positive"],
    },
    {
        "category": "SHAP factor attribution",
        "question": "Does the freehold tenure help the valuation?",
        "must_mention_any": ["freehold", "63", "yes", "positive", "adds"],
        "must_not_contradict": ["freehold is negative", "no effect", "doesn't matter"],
    },

    # ── Category 3: Price range interpretation (4 cases) ──────────────────────
    {
        "category": "Price range interpretation",
        "question": "What is the confidence interval for this estimate?",
        "must_mention_any": ["1.48", "1.94", "13.4", "range", "MAPE"],
        "must_not_contradict": ["exact", "certain", "no uncertainty"],
    },
    {
        "category": "Price range interpretation",
        "question": "If I offer S$1.75M, is that within the fair range?",
        "must_mention_any": ["1.48", "1.94", "within", "range", "yes", "reasonable"],
        "must_not_contradict": ["outside range", "too low", "1.75 is too high"],
    },
    {
        "category": "Price range interpretation",
        "question": "How accurate is the AI valuation?",
        "must_mention_any": ["13.4", "MAPE", "13%", "uncertainty", "estimate"],
        "must_not_contradict": ["100% accurate", "exact", "perfect", "no error"],
    },
    {
        "category": "Price range interpretation",
        "question": "What's the lowest reasonable offer I could make?",
        "must_mention_any": ["1.48", "1.94", "range", "negotiate", "estimate"],
        "must_not_contradict": ["2 million is fair", "don't go below 2"],
    },

    # ── Category 4: Market comparison (2 cases) ───────────────────────────────
    {
        "category": "Market comparison",
        "question": "How does this compare to other condos in District 9?",
        "must_mention_any": ["district 9", "robertson", "orchard", "overpriced", "location"],
        "must_not_contradict": ["district 9 is cheap", "below market"],
    },
    {
        "category": "Market comparison",
        "question": "Is Robertson Quay a good location for a condo?",
        "must_mention_any": ["district 9", "location", "187", "premium", "robertson"],
        "must_not_contradict": ["bad location", "district 9 is poor", "negative location"],
    },
    # ── Category 5: Renovation / condition inference (3 cases) ────────────────
    {
        "category": "Renovation inference",
        "question": "Is the unit likely renovated given it's 21 years old?",
        "must_mention_any": ["2005", "21", "age", "renovation", "uncertain", "unknown", "cannot", "don't know"],
        "must_not_contradict": [],
    },
    {
        "category": "Renovation inference",
        "question": "How much would renovation cost affect the fair value?",
        "must_mention_any": ["renovation", "condition", "uncertain", "not captured", "unknown", "cannot", "listing"],
        "must_not_contradict": [],
    },
    {
        "category": "Renovation inference",
        "question": "Would a newly renovated unit change the valuation?",
        "must_mention_any": ["renovation", "condition", "not captured", "model", "uncertain", "structural"],
        "must_not_contradict": [],
    },
]

TEST_CASES_HDB = [
    # ── Valuation verdict (4 cases) ───────────────────────────────────────────
    {
        "category": "Valuation verdict",
        "question": "Is this HDB flat priced fairly for rent?",
        "must_mention_any": ["overpriced", "overprice", "3,200", "2,880", "11%", "above", "high"],
        "must_not_contradict": ["below the estimate", "underpriced", "below market rate"],
    },
    {
        "category": "Valuation verdict",
        "question": "How much above market rate is the asking rent?",
        "must_mention_any": ["320", "11%", "3,200", "2,880", "overpriced", "above"],
        "must_not_contradict": ["below market rate", "underpriced", "asking is lower than estimate"],
    },
    {
        "category": "Valuation verdict",
        "question": "Should I try to negotiate the rent down?",
        "must_mention_any": ["overpriced", "2,880", "negotiate", "11%", "estimate"],
        "must_not_contradict": ["do not negotiate", "no room to negotiate", "asking is fair value"],
    },
    {
        "category": "Valuation verdict",
        "question": "What rent should I expect to pay for a 4-room HDB in Toa Payoh?",
        "must_mention_any": ["2,880", "2,624", "3,136", "range", "estimate"],
        "must_not_contradict": ["3,200 is the fair value", "no estimate available"],
    },
    # ── SHAP factor attribution (3 cases) ─────────────────────────────────────
    {
        "category": "SHAP factor attribution",
        "question": "Why is the valuation lower than the asking rent?",
        "must_mention_any": ["age", "38", "1988", "district 12", "290", "180"],
        "must_not_contradict": ["bedroom is negative", "floor area is negative"],
    },
    {
        "category": "SHAP factor attribution",
        "question": "What is the biggest factor boosting the valuation?",
        "must_mention_any": ["bedroom", "4 bedroom", "420", "floor area", "310"],
        "must_not_contradict": ["district is biggest", "age is positive", "tenure is biggest"],
    },
    {
        "category": "SHAP factor attribution",
        "question": "Does the age of this HDB block affect the rent?",
        "must_mention_any": ["38", "1988", "age", "290", "older", "lease", "negative"],
        "must_not_contradict": ["age has no effect", "age is positive", "age helps"],
    },
    # ── Price range interpretation (3 cases) ──────────────────────────────────
    {
        "category": "Price range interpretation",
        "question": "How confident is the AI in this rental estimate?",
        "must_mention_any": ["8.9", "MAPE", "8%", "9%", "range", "2,624", "3,136"],
        "must_not_contradict": ["100% accurate", "exactly right", "no uncertainty", "certain"],
    },
    {
        "category": "Price range interpretation",
        "question": "If I counter-offer at S$3,000/month, is that reasonable?",
        "must_mention_any": ["2,624", "3,136", "range", "within", "reasonable", "estimate"],
        "must_not_contradict": ["3,000 is outside the range", "3,000 is too low", "offer is unreasonable"],
    },
    {
        "category": "Price range interpretation",
        "question": "What is the estimated fair rent range for this flat?",
        "must_mention_any": ["2,624", "3,136", "range", "2,880"],
        "must_not_contradict": ["3,200 is the fair value", "no range available"],
    },
    # ── Market comparison (1 case) ────────────────────────────────────────────
    {
        "category": "Market comparison",
        "question": "Is Toa Payoh a good location for renting an HDB?",
        "must_mention_any": ["district 12", "toa payoh", "location", "central", "mature"],
        "must_not_contradict": ["bad location", "poor area", "district 12 is premium"],
    },
    # ── Renovation inference (1 case) ─────────────────────────────────────────
    {
        "category": "Renovation inference",
        "question": "This HDB was built in 1988. Should I worry about its condition?",
        "must_mention_any": ["1988", "38", "age", "condition", "older", "renovation", "unknown"],
        "must_not_contradict": [],
    },
]

TEST_CASES_LANDED = [
    # ── Valuation verdict (4 cases) ───────────────────────────────────────────
    {
        "category": "Valuation verdict",
        "question": "Is this landed property overpriced at S$4.8M?",
        "must_mention_any": ["overpriced", "4.15", "15%", "above", "estimate"],
        "must_not_contradict": ["below the estimate", "undervalued", "4.8 is the fair value"],
    },
    {
        "category": "Valuation verdict",
        "question": "How does the asking price compare to the AI estimate?",
        "must_mention_any": ["4.15", "4.8", "15%", "overpriced", "650", "above"],
        "must_not_contradict": ["asking is below estimate", "asking is lower than 4.15"],
    },
    {
        "category": "Valuation verdict",
        "question": "What is the estimated value of this semi-detached house?",
        "must_mention_any": ["4.15", "3.06", "5.24", "estimate", "range"],
        "must_not_contradict": ["4.8 is the estimated value", "no estimate available"],
    },
    {
        "category": "Valuation verdict",
        "question": "Should I be worried about the wide price range shown?",
        "must_mention_any": ["26", "MAPE", "26.3", "range", "landed", "uncertain", "wide"],
        "must_not_contradict": ["the range is narrow", "valuation is very precise", "no uncertainty"],
    },
    # ── SHAP factor attribution (3 cases) ─────────────────────────────────────
    {
        "category": "SHAP factor attribution",
        "question": "What is pulling the valuation below the asking price?",
        "must_mention_any": ["age", "31", "1995", "380", "property age", "negative"],
        "must_not_contradict": ["floor area is negative", "freehold is negative", "district is negative"],
    },
    {
        "category": "SHAP factor attribution",
        "question": "What are the key value drivers for this landed property?",
        "must_mention_any": ["floor area", "820", "freehold", "410", "district", "290"],
        "must_not_contradict": ["age is biggest positive", "bedroom is biggest"],
    },
    {
        "category": "SHAP factor attribution",
        "question": "Does the freehold tenure significantly boost the value?",
        "must_mention_any": ["freehold", "410", "yes", "positive", "adds"],
        "must_not_contradict": ["freehold is negative", "no effect", "doesn't matter"],
    },
    # ── Price range interpretation (3 cases) ──────────────────────────────────
    {
        "category": "Price range interpretation",
        "question": "Why is the price range so wide for this property?",
        "must_mention_any": ["26", "MAPE", "landed", "heterogeneous", "uncertain", "wide", "26.3"],
        "must_not_contradict": ["the range is narrow", "valuation is very precise", "no uncertainty"],
    },
    {
        "category": "Price range interpretation",
        "question": "If I offer S$4.2M, is that within the estimated fair range?",
        "must_mention_any": ["3.06", "5.24", "range", "within", "yes", "reasonable", "4.2"],
        "must_not_contradict": ["4.2 is outside the range", "4.2 is below the fair range"],
    },
    {
        "category": "Price range interpretation",
        "question": "How reliable is the AI valuation for landed properties?",
        "must_mention_any": ["26", "MAPE", "26.3", "uncertain", "less reliable", "landed", "wide"],
        "must_not_contradict": ["highly accurate", "exactly right", "very reliable", "no uncertainty"],
    },
    # ── Market comparison (1 case) ────────────────────────────────────────────
    {
        "category": "Market comparison",
        "question": "Is Bukit Timah a premium location for landed housing?",
        "must_mention_any": ["bukit timah", "district 21", "location", "premium", "290"],
        "must_not_contradict": ["bad location", "poor area", "cheap area"],
    },
    # ── Renovation inference (1 case) ─────────────────────────────────────────
    {
        "category": "Renovation inference",
        "question": "The house was built in 1995. Does the age significantly affect value?",
        "must_mention_any": ["1995", "31", "380", "age", "negative", "depreciation"],
        "must_not_contradict": ["age has no effect", "age is positive"],
    },
]

# Combined test suite with segment labels
TEST_SUITE = (
    [{"segment": "Condo Sale", **c} for c in TEST_CASES_CONDO] +
    [{"segment": "HDB Rent",   **c} for c in TEST_CASES_HDB] +
    [{"segment": "Landed Sale", **c} for c in TEST_CASES_LANDED]
)

# Keep backward-compat alias
TEST_CASES = TEST_CASES_CONDO

# ── scoring ───────────────────────────────────────────────────────────────────
def score(response: str, case: dict) -> tuple[int, int]:
    """Returns (factual_consistency, relevance) as 0 or 1."""
    r = response.lower()

    # factual consistency: fail if any contradiction phrase appears
    consistency = 1
    for phrase in case.get("must_not_contradict", []):
        if phrase.lower() in r:
            consistency = 0
            break

    # relevance: pass if at least one expected keyword appears
    relevance = 0
    for kw in case.get("must_mention_any", []):
        if kw.lower() in r:
            relevance = 1
            break

    return consistency, relevance


# ── main ─────────────────────────────────────────────────────────────────────
def run_eval():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set. Add it to backend/.env or export it.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    # map segment name → system prompt
    segment_prompts = {
        "Condo Sale":  make_system_prompt(LISTING_CONTEXT_CONDO,   VALUATION_CONTEXT_CONDO),
        "HDB Rent":    make_system_prompt(LISTING_CONTEXT_HDB,     VALUATION_CONTEXT_HDB),
        "Landed Sale": make_system_prompt(LISTING_CONTEXT_LANDED,  VALUATION_CONTEXT_LANDED),
    }

    results = []
    category_stats: dict[str, dict] = {}
    segment_stats:  dict[str, dict] = {}

    print(f"\n{'='*70}")
    print(f"Chat Assistant Evaluation  ({len(TEST_SUITE)} interactions, 3 segments)")
    print(f"{'='*70}\n")

    for i, case in enumerate(TEST_SUITE, 1):
        cat     = case["category"]
        q       = case["question"]
        segment = case["segment"]
        prompt  = segment_prompts[segment]

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=200,
                system=prompt,
                messages=[{"role": "user", "content": q}],
            )
            answer = resp.content[0].text.strip()
        except Exception as e:
            print(f"  [{i:02d}] API error: {e}")
            answer = ""

        fc, rel = score(answer, case)

        results.append({
            "id": i,
            "segment": segment,
            "category": cat,
            "question": q,
            "response": answer,
            "factual_consistency": fc,
            "relevance": rel,
        })

        # category stats
        if cat not in category_stats:
            category_stats[cat] = {"n": 0, "fc": 0, "rel": 0}
        category_stats[cat]["n"]   += 1
        category_stats[cat]["fc"]  += fc
        category_stats[cat]["rel"] += rel

        # segment stats
        if segment not in segment_stats:
            segment_stats[segment] = {"n": 0, "fc": 0, "rel": 0}
        segment_stats[segment]["n"]   += 1
        segment_stats[segment]["fc"]  += fc
        segment_stats[segment]["rel"] += rel

        mark_fc  = "✅" if fc  else "❌"
        mark_rel = "✅" if rel else "❌"
        print(f"  [{i:02d}] [{segment:<12}] [{cat[:22]:<22}] FC:{mark_fc} Rel:{mark_rel}")
        print(f"       Q: {q}")
        print(f"       A: {answer[:120]}{'...' if len(answer)>120 else ''}\n")

        time.sleep(0.4)   # gentle rate limit

    # ── Summary ───────────────────────────────────────────────────────────────
    total_fc  = sum(r["factual_consistency"] for r in results)
    total_rel = sum(r["relevance"]           for r in results)
    n         = len(results)

    print(f"\n{'='*70}")
    print(f"SEGMENT BREAKDOWN")
    print(f"{'Segment':<16} {'N':>3}  {'FC':>10}  {'Relevance':>10}")
    print("-" * 44)
    for seg, s in segment_stats.items():
        print(f"  {seg:<14} {s['n']:>3}  "
              f"{s['fc']}/{s['n']} ({100*s['fc']//s['n']:3d}%)  "
              f"{s['rel']}/{s['n']} ({100*s['rel']//s['n']:3d}%)")

    print(f"\nCATEGORY BREAKDOWN")
    print(f"{'Category':<26} {'N':>3}  {'FC':>10}  {'Relevance':>10}")
    print("-" * 54)
    for cat, s in category_stats.items():
        print(f"  {cat:<24} {s['n']:>3}  "
              f"{s['fc']}/{s['n']} ({100*s['fc']//s['n']:3d}%)  "
              f"{s['rel']}/{s['n']} ({100*s['rel']//s['n']:3d}%)")

    print("-" * 54)
    print(f"  {'OVERALL':<24} {n:>3}  "
          f"{total_fc}/{n} ({100*total_fc//n:3d}%)  "
          f"{total_rel}/{n} ({100*total_rel//n:3d}%)\n")

    summary = {
        "n_total": n,
        "total_factual_consistency": total_fc,
        "total_relevance": total_rel,
        "pct_factual_consistency": round(100 * total_fc / n, 1),
        "pct_relevance": round(100 * total_rel / n, 1),
        "by_segment": {
            seg: {
                "n": s["n"],
                "factual_consistency": f"{s['fc']}/{s['n']} ({100*s['fc']//s['n']}%)",
                "relevance": f"{s['rel']}/{s['n']} ({100*s['rel']//s['n']}%)",
            }
            for seg, s in segment_stats.items()
        },
        "by_category": {
            cat: {
                "n": s["n"],
                "factual_consistency": f"{s['fc']}/{s['n']} ({100*s['fc']//s['n']}%)",
                "relevance": f"{s['rel']}/{s['n']} ({100*s['rel']//s['n']}%)",
            }
            for cat, s in category_stats.items()
        },
        "interactions": results,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Results saved: {OUTPUT_PATH}")

    return summary


if __name__ == "__main__":
    run_eval()
