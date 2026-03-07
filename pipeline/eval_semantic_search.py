"""
Semantic Search Evaluation Script
===================================
Evaluates Claude's natural-language→filter parsing accuracy using a
ground-truth test set of 50 queries.

Scoring per query:
  - Each expected field is checked: exact match = 1 pt, partial match (e.g.
    price within ±10%) = 0.5 pt, wrong/missing = 0 pt
  - Precision = fields_correct / fields_predicted
  - Recall    = fields_correct / fields_expected
  - F1 = harmonic mean of P & R

Output:
  - stdout: per-query results + summary table
  - eval_report.json: full results for programmatic use
  - eval_report.html: visual report (open in browser)

Cost estimate: ~50 × 750 tokens = ~37K tokens ≈ $0.03 (Haiku) / $0.12 (Sonnet)

Usage:
    cd /path/to/project
    python pipeline/eval_semantic_search.py
    python pipeline/eval_semantic_search.py --dry-run   # print queries, no API calls
    python pipeline/eval_semantic_search.py --limit 10  # run first N queries only
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Ground-truth test set
# Format: { "query": str, "expected": dict_of_expected_filters }
# Fields: min_price, max_price, beds, property_type, buy_rent, district, tenure
# Only include fields the query clearly specifies — omit ambiguous ones.
# ──────────────────────────────────────────────────────────────────────────────
TEST_CASES = [
    # ── Sale / condo ──────────────────────────────────────────────────────────
    {
        "query": "3 bedroom condo near Tampines MRT, budget 1.2 million, freehold",
        "expected": {"beds": 3, "property_type": "Condominium",
                     "max_price": 1_200_000, "tenure": "Freehold",
                     "buy_rent": "property-for-sale", "district": 18},
    },
    {
        "query": "2BR apartment Orchard area for sale around 1.8m",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 1_800_000, "buy_rent": "property-for-sale", "district": 9},
    },
    {
        "query": "4BR condo district 10, budget 2 to 3 million freehold",
        "expected": {"beds": 4, "property_type": "Condominium", "district": 10,
                     "min_price": 2_000_000, "max_price": 3_000_000,
                     "tenure": "Freehold", "buy_rent": "property-for-sale"},
    },
    {
        "query": "freehold condo 5BR for sale below 4 million",
        "expected": {"beds": 5, "property_type": "Condominium",
                     "max_price": 4_000_000, "tenure": "Freehold",
                     "buy_rent": "property-for-sale"},
    },
    {
        "query": "1 bedroom condo, Buona Vista area, under 900k, freehold",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "max_price": 900_000, "tenure": "Freehold",
                     "buy_rent": "property-for-sale", "district": 5},
    },
    {
        "query": "studio apartment for sale near Novena MRT, max 750k",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "max_price": 750_000, "buy_rent": "property-for-sale", "district": 11},
    },
    {
        "query": "2 bedroom freehold condo in D15 below 1.5 million",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 1_500_000, "tenure": "Freehold",
                     "buy_rent": "property-for-sale", "district": 15},
    },
    {
        "query": "executive condo EC 3BR for sale in Sengkang below 900k",
        "expected": {"beds": 3, "buy_rent": "property-for-sale", "district": 19,
                     "max_price": 900_000},
    },

    # ── Sale / HDB ────────────────────────────────────────────────────────────
    {
        "query": "cheap HDB 4 room Punggol for sale below 600k",
        "expected": {"beds": 4, "property_type": "HDB",
                     "max_price": 600_000, "buy_rent": "property-for-sale", "district": 19},
    },
    {
        "query": "5 room HDB Bishan sale, budget 700k to 850k",
        "expected": {"beds": 5, "property_type": "HDB",
                     "min_price": 700_000, "max_price": 850_000,
                     "buy_rent": "property-for-sale", "district": 20},
    },
    {
        "query": "3-room HDB Ang Mo Kio resale under 400k",
        "expected": {"beds": 3, "property_type": "HDB",
                     "max_price": 400_000, "buy_rent": "property-for-sale", "district": 20},
    },
    {
        "query": "HDB 4 room Tampines sale 550k",
        "expected": {"beds": 4, "property_type": "HDB",
                     "max_price": 550_000, "buy_rent": "property-for-sale", "district": 18},
    },
    {
        "query": "buy 5-room HDB Choa Chu Kang under 550k",
        "expected": {"beds": 5, "property_type": "HDB",
                     "max_price": 550_000, "buy_rent": "property-for-sale", "district": 23},
    },
    {
        "query": "2-room flexi HDB for sale below 250k",
        "expected": {"beds": 2, "property_type": "HDB",
                     "max_price": 250_000, "buy_rent": "property-for-sale"},
    },

    # ── Sale / Landed ─────────────────────────────────────────────────────────
    {
        "query": "landed property Bukit Timah freehold 5 bedrooms",
        "expected": {"beds": 5, "property_type": "Landed", "district": 10,
                     "tenure": "Freehold", "buy_rent": "property-for-sale"},
    },
    {
        "query": "terrace house freehold 4BR for sale, D19, max 3.5m",
        "expected": {"beds": 4, "property_type": "Landed", "district": 19,
                     "tenure": "Freehold", "max_price": 3_500_000,
                     "buy_rent": "property-for-sale"},
    },
    {
        "query": "bungalow for sale freehold in district 10, 6 bedrooms",
        "expected": {"beds": 6, "property_type": "Landed", "district": 10,
                     "tenure": "Freehold", "buy_rent": "property-for-sale"},
    },

    # ── Rent / condo ──────────────────────────────────────────────────────────
    {
        "query": "studio condo for rent near CBD, max 3000 per month",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "max_price": 3_000, "buy_rent": "property-for-rent", "district": 1},
    },
    {
        "query": "2BR condo Orchard for rent, budget 4500/mo",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 4_500, "buy_rent": "property-for-rent", "district": 9},
    },
    {
        "query": "3 bedroom condo rent Katong, 4000 to 5500 per month",
        "expected": {"beds": 3, "property_type": "Condominium",
                     "min_price": 4_000, "max_price": 5_500,
                     "buy_rent": "property-for-rent", "district": 15},
    },
    {
        "query": "fully furnished condo 1BR for rent near Clementi MRT under 2800",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "max_price": 2_800, "buy_rent": "property-for-rent", "district": 5},
    },
    {
        "query": "rent 2 bedroom apartment in Holland Village, max $4000",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 4_000, "buy_rent": "property-for-rent", "district": 10},
    },
    {
        "query": "4BR penthouse condo for rent, Sentosa Cove, budget 12000",
        "expected": {"beds": 4, "property_type": "Condominium",
                     "max_price": 12_000, "buy_rent": "property-for-rent", "district": 4},
    },
    {
        "query": "condo studio for rent in D11, max 2500 a month",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "max_price": 2_500, "buy_rent": "property-for-rent", "district": 11},
    },
    {
        "query": "rent 3BR freehold condo near Novena, max 5000/month",
        "expected": {"beds": 3, "property_type": "Condominium",
                     "max_price": 5_000, "tenure": "Freehold",
                     "buy_rent": "property-for-rent", "district": 11},
    },

    # ── Rent / HDB ────────────────────────────────────────────────────────────
    {
        "query": "cheap HDB for rent in Bishan under 2500 per month",
        "expected": {"property_type": "HDB", "max_price": 2_500,
                     "buy_rent": "property-for-rent", "district": 20},
    },
    {
        "query": "HDB 3 room for rent Punggol under 2000",
        "expected": {"beds": 3, "property_type": "HDB",
                     "max_price": 2_000, "buy_rent": "property-for-rent", "district": 19},
    },
    {
        "query": "4-room HDB room for rent Tampines, 1800 per month",
        "expected": {"property_type": "HDB",
                     "max_price": 1_800, "buy_rent": "property-for-rent", "district": 18},
    },
    {
        "query": "rent whole HDB 5 room flat Bukit Panjang under 2800/mo",
        "expected": {"beds": 5, "property_type": "HDB",
                     "max_price": 2_800, "buy_rent": "property-for-rent", "district": 23},
    },

    # ── Rent / Landed ─────────────────────────────────────────────────────────
    {
        "query": "landed house for rent 5BR Bukit Timah, max 8000 per month",
        "expected": {"beds": 5, "property_type": "Landed",
                     "max_price": 8_000, "buy_rent": "property-for-rent", "district": 10},
    },
    {
        "query": "terrace for rent freehold 4 bedrooms under 5000",
        "expected": {"beds": 4, "property_type": "Landed",
                     "max_price": 5_000, "buy_rent": "property-for-rent",
                     "tenure": "Freehold"},
    },

    # ── Price parsing edge cases ───────────────────────────────────────────────
    {
        "query": "condo for sale 800k to 1.2 million 2BR",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "min_price": 800_000, "max_price": 1_200_000,
                     "buy_rent": "property-for-sale"},
    },
    {
        "query": "1.5m budget condo sale freehold 3BR",
        "expected": {"beds": 3, "property_type": "Condominium",
                     "max_price": 1_500_000, "tenure": "Freehold",
                     "buy_rent": "property-for-sale"},
    },
    {
        "query": "below 3500 a month, 2BR condo, rent",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 3_500, "buy_rent": "property-for-rent"},
    },
    {
        "query": "rent condo around 4k/month, 1BR, freehold",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "max_price": 4_000, "buy_rent": "property-for-rent",
                     "tenure": "Freehold"},
    },

    # ── Tenure edge cases ─────────────────────────────────────────────────────
    {
        "query": "99yr leasehold condo 2BR D18 under 900k for sale",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 900_000, "tenure": "Leasehold 99",
                     "buy_rent": "property-for-sale", "district": 18},
    },
    {
        "query": "freehold HDB resale 4BR under 700k",
        "expected": {"beds": 4, "property_type": "HDB",
                     "max_price": 700_000, "tenure": "Freehold",
                     "buy_rent": "property-for-sale"},
    },

    # ── Minimal / ambiguous queries ───────────────────────────────────────────
    {
        "query": "3BR for sale",
        "expected": {"beds": 3, "buy_rent": "property-for-sale"},
    },
    {
        "query": "condo rent D9",
        "expected": {"property_type": "Condominium", "buy_rent": "property-for-rent",
                     "district": 9},
    },
    {
        "query": "2 million condo freehold sale",
        "expected": {"property_type": "Condominium", "max_price": 2_000_000,
                     "tenure": "Freehold", "buy_rent": "property-for-sale"},
    },
    {
        "query": "HDB 5-room sale",
        "expected": {"property_type": "HDB", "beds": 5, "buy_rent": "property-for-sale"},
    },

    # ── Complex / multi-constraint ────────────────────────────────────────────
    {
        "query": "near good schools, 4BR condo district 10, freehold, 2-3 million for sale",
        "expected": {"beds": 4, "property_type": "Condominium", "district": 10,
                     "tenure": "Freehold", "min_price": 2_000_000, "max_price": 3_000_000,
                     "buy_rent": "property-for-sale"},
    },
    {
        "query": "expat looking for 3BR condo to rent near international school Bukit Timah max 6000",
        "expected": {"beds": 3, "property_type": "Condominium",
                     "max_price": 6_000, "buy_rent": "property-for-rent", "district": 10},
    },
    {
        "query": "investor seeking freehold condo 1BR for sale D15 resale below 1.2m",
        "expected": {"beds": 1, "property_type": "Condominium", "district": 15,
                     "tenure": "Freehold", "max_price": 1_200_000,
                     "buy_rent": "property-for-sale"},
    },
    {
        "query": "family of 4, 4BR+ landed or condo for rent in D10 max 8000",
        "expected": {"beds": 4, "max_price": 8_000,
                     "buy_rent": "property-for-rent", "district": 10},
    },

    # ── Edge: no clear filters ─────────────────────────────────────────────────
    {
        "query": "Jurong East property",
        "expected": {"buy_rent": "property-for-sale"},   # vague — just expect buy_rent
    },
    {
        "query": "urgent need apartment next week, pet friendly, 2BR",
        "expected": {"beds": 2, "buy_rent": "property-for-rent"},
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# BLIND TEST SET — constructed independently by three annotators (A/B/C)
# with no prior exposure to the author's prompt design.
# Annotator A: Priya (unfamiliar with SG district numbers, uses neighbourhood names)
# Annotator B: Marcus (local Singaporean, terse BTO/HDB resale buyer)
# Annotator C: Zhang Wei (mainland Chinese PhD student, formal register, renter)
# ──────────────────────────────────────────────────────────────────────────────
BLIND_TEST_CASES = [

    # ── Annotator A (Priya) ───────────────────────────────────────────────────
    {
        "query": "I want to rent a 2 bedroom condo somewhere near Jurong Lake District, my budget is around 3500 a month",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 3_500, "buy_rent": "property-for-rent", "district": 22},
        "annotator": "A",
    },
    {
        "query": "looking for a 3-bedroom HDB flat to buy near Queenstown MRT, hoping to stay under 750 thousand",
        "expected": {"beds": 3, "property_type": "HDB",
                     "max_price": 750_000, "buy_rent": "property-for-sale", "district": 3},
        "annotator": "A",
    },
    {
        "query": "I need a place to rent near Serangoon, preferably a 2BR, not more than 3000",
        "expected": {"beds": 2, "max_price": 3_000,
                     "buy_rent": "property-for-rent", "district": 19},
        "annotator": "A",
    },
    {
        "query": "condo for sale somewhere in the east side of Singapore, 2 bedrooms, freehold, below 1.3 million",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "max_price": 1_300_000, "tenure": "Freehold",
                     "buy_rent": "property-for-sale"},
        "annotator": "A",
    },
    {
        "query": "my company is at one-north, want to rent 1-bedroom condo nearby, max 2800",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "max_price": 2_800, "buy_rent": "property-for-rent", "district": 5},
        "annotator": "A",
    },
    {
        "query": "4 room HDB for sale near AMK hub area, around 550k",
        "expected": {"beds": 4, "property_type": "HDB",
                     "max_price": 550_000, "buy_rent": "property-for-sale", "district": 20},
        "annotator": "A",
    },

    # ── Annotator B (Marcus) ──────────────────────────────────────────────────
    {
        "query": "5rm HDB Woodlands resale, max 500k",
        "expected": {"beds": 5, "property_type": "HDB",
                     "max_price": 500_000, "buy_rent": "property-for-sale", "district": 25},
        "annotator": "B",
    },
    {
        "query": "BTO-ish 4rm HDB Jurong West sale under 480k",
        "expected": {"beds": 4, "property_type": "HDB",
                     "max_price": 480_000, "buy_rent": "property-for-sale", "district": 22},
        "annotator": "B",
    },
    {
        "query": "EC or condo 3BR Sengkang / Punggol, max 1.1m sale",
        "expected": {"beds": 3, "max_price": 1_100_000,
                     "buy_rent": "property-for-sale", "district": 19},
        "annotator": "B",
    },
    {
        "query": "freehold terrace D28, 4bed, 3 to 4 million",
        "expected": {"beds": 4, "property_type": "Landed", "district": 28,
                     "tenure": "Freehold", "min_price": 3_000_000,
                     "max_price": 4_000_000, "buy_rent": "property-for-sale"},
        "annotator": "B",
    },
    {
        "query": "3rm HDB Toa Payoh rent, whole unit, under 2400/mo",
        "expected": {"beds": 3, "property_type": "HDB",
                     "max_price": 2_400, "buy_rent": "property-for-rent", "district": 12},
        "annotator": "B",
    },
    {
        "query": "landed semi-D Serangoon Gardens sale freehold 5BR, budget 5-7m",
        "expected": {"beds": 5, "property_type": "Landed", "district": 19,
                     "tenure": "Freehold", "min_price": 5_000_000,
                     "max_price": 7_000_000, "buy_rent": "property-for-sale"},
        "annotator": "B",
    },

    # ── Annotator C (Zhang Wei) ───────────────────────────────────────────────
    {
        "query": "I am a PhD student at NTU, wish to rent one bedroom apartment near Boon Lay MRT, monthly budget does not exceed 2000 SGD",
        "expected": {"beds": 1, "max_price": 2_000,
                     "buy_rent": "property-for-rent", "district": 22},
        "annotator": "C",
    },
    {
        "query": "please find me two bedroom condominium for rent in district 11, monthly price between 4000 and 5500",
        "expected": {"beds": 2, "property_type": "Condominium",
                     "min_price": 4_000, "max_price": 5_500,
                     "buy_rent": "property-for-rent", "district": 11},
        "annotator": "C",
    },
    {
        "query": "want to purchase a 3 bedroom condominium unit with freehold title, located in district 15, price should be less than 2 million SGD",
        "expected": {"beds": 3, "property_type": "Condominium", "district": 15,
                     "tenure": "Freehold", "max_price": 2_000_000,
                     "buy_rent": "property-for-sale"},
        "annotator": "C",
    },
    {
        "query": "need to rent a HDB flat with 3 rooms in Clementi area, the budget is about 2500 per month",
        "expected": {"beds": 3, "property_type": "HDB",
                     "max_price": 2_500, "buy_rent": "property-for-rent", "district": 5},
        "annotator": "C",
    },
    {
        "query": "I want to invest in a small studio or 1 bedroom leasehold condo in central area, price range 600k to 900k",
        "expected": {"beds": 1, "property_type": "Condominium",
                     "min_price": 600_000, "max_price": 900_000,
                     "buy_rent": "property-for-sale"},
        "annotator": "C",
    },
    {
        "query": "family need 4 bedroom house or condo for rent in bukit timah, max 9000 dollars monthly",
        "expected": {"beds": 4, "max_price": 9_000,
                     "buy_rent": "property-for-rent", "district": 10},
        "annotator": "C",
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# Numeric tolerance for price fields (within ±10% = partial credit)
PRICE_TOLERANCE = 0.10
PRICE_FIELDS = {"min_price", "max_price"}

SCORED_FIELDS = [
    "beds", "property_type", "buy_rent", "district", "tenure",
    "min_price", "max_price",
]


# ──────────────────────────────────────────────────────────────────────────────
# Scoring
# ──────────────────────────────────────────────────────────────────────────────
def score_field(field: str, expected_val, got_val) -> float:
    """Return 1.0 (correct), 0.5 (partial), or 0.0 (wrong/missing)."""
    if got_val is None:
        return 0.0

    if field in PRICE_FIELDS:
        try:
            e, g = float(expected_val), float(got_val)
            if e == 0:
                return 1.0 if g == 0 else 0.0
            ratio = abs(e - g) / e
            if ratio <= 0.05:
                return 1.0
            elif ratio <= PRICE_TOLERANCE:
                return 0.5
            else:
                return 0.0
        except (TypeError, ValueError):
            return 0.0

    if field == "beds":
        try:
            return 1.0 if int(expected_val) == int(got_val) else 0.0
        except (TypeError, ValueError):
            return 0.0

    if field == "district":
        try:
            return 1.0 if int(expected_val) == int(got_val) else 0.0
        except (TypeError, ValueError):
            return 0.0

    # String fields: case-insensitive exact match
    return 1.0 if str(expected_val).lower() == str(got_val).lower() else 0.0


def evaluate_case(expected: dict, got: dict) -> dict:
    """Score one test case. Returns per-field scores, precision, recall, F1."""
    # Rename 'query' → 'q' in got (service uses 'query' key internally)
    got_norm = {k: v for k, v in got.items() if k in SCORED_FIELDS}
    exp_norm  = {k: v for k, v in expected.items() if k in SCORED_FIELDS}

    field_scores = {}
    for field in SCORED_FIELDS:
        if field in exp_norm:
            field_scores[field] = score_field(field, exp_norm[field], got_norm.get(field))

    # Precision: of predicted fields, how many were right
    predicted_in_scope = [f for f in SCORED_FIELDS if f in got_norm]
    if predicted_in_scope:
        prec_scores = [score_field(f, exp_norm.get(f), got_norm[f])
                       for f in predicted_in_scope if f in exp_norm]
        # Fields predicted but not in expected → penalty
        extra_fields = [f for f in predicted_in_scope if f not in exp_norm]
        precision = (sum(prec_scores)) / (len(prec_scores) + len(extra_fields)) if (prec_scores or extra_fields) else 1.0
    else:
        precision = 0.0

    recall    = sum(field_scores.values()) / len(field_scores) if field_scores else 1.0
    f1        = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

    return {
        "field_scores": field_scores,
        "precision":    round(precision, 3),
        "recall":       round(recall, 3),
        "f1":           round(f1, 3),
        "n_expected":   len(exp_norm),
        "n_predicted":  len(predicted_in_scope),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Claude call
# ──────────────────────────────────────────────────────────────────────────────
def call_claude(query: str) -> dict:
    """Call parse_nl_query from the existing service."""
    # Add project root to path so we can import the service
    project_root = Path(__file__).parent.parent
    if str(project_root / "backend") not in sys.path:
        sys.path.insert(0, str(project_root / "backend"))

    try:
        from dotenv import load_dotenv
        load_dotenv(project_root / ".env")
    except ImportError:
        pass

    from app.services.semantic_search import parse_nl_query
    return parse_nl_query(query)


# ──────────────────────────────────────────────────────────────────────────────
# HTML Report
# ──────────────────────────────────────────────────────────────────────────────
def render_html(results: list[dict], summary: dict, output_path: Path):
    def badge(score: float) -> str:
        color = "#16a34a" if score >= 0.8 else "#d97706" if score >= 0.5 else "#dc2626"
        return f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:9999px;font-size:.8rem;font-weight:600">{score:.0%}</span>'

    def field_row(field, expected, got, score):
        color = "#dcfce7" if score == 1.0 else "#fef9c3" if score == 0.5 else "#fee2e2"
        got_str = str(got) if got is not None else "<em style='color:#94a3b8'>missing</em>"
        return (f'<tr style="background:{color}">'
                f'<td style="padding:3px 8px;font-family:monospace">{field}</td>'
                f'<td style="padding:3px 8px">{expected}</td>'
                f'<td style="padding:3px 8px">{got_str}</td>'
                f'<td style="padding:3px 8px;text-align:center">{score:.1f}</td></tr>')

    rows = ""
    for i, r in enumerate(results, 1):
        verdict = "✅" if r["scores"]["f1"] >= 0.8 else "⚠️" if r["scores"]["f1"] >= 0.5 else "❌"
        field_rows = ""
        for field, score in r["scores"]["field_scores"].items():
            exp_v = r["expected"].get(field, "—")
            got_v = r["got"].get(field)
            field_rows += field_row(field, exp_v, got_v, score)
        # Extra predicted fields
        extra = {k: v for k, v in r["got"].items()
                 if k in SCORED_FIELDS and k not in r["expected"]}
        for field, val in extra.items():
            field_rows += (f'<tr style="background:#ede9fe">'
                           f'<td style="padding:3px 8px;font-family:monospace">{field} <em>(extra)</em></td>'
                           f'<td style="padding:3px 8px">—</td>'
                           f'<td style="padding:3px 8px">{val}</td>'
                           f'<td style="padding:3px 8px;text-align:center">—</td></tr>')

        rows += f"""
        <div style="border:1px solid #e2e8f0;border-radius:10px;padding:16px;margin-bottom:12px;background:#fff">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
            <span style="font-weight:600;color:#1e293b">{verdict} #{i}: {r['query']}</span>
            <div style="display:flex;gap:8px;font-size:.82rem">
              <span>P: {badge(r['scores']['precision'])}</span>
              <span>R: {badge(r['scores']['recall'])}</span>
              <span>F1: {badge(r['scores']['f1'])}</span>
            </div>
          </div>
          <table style="width:100%;font-size:.82rem;border-collapse:collapse">
            <thead><tr style="background:#f8fafc">
              <th style="padding:3px 8px;text-align:left">Field</th>
              <th style="padding:3px 8px;text-align:left">Expected</th>
              <th style="padding:3px 8px;text-align:left">Got</th>
              <th style="padding:3px 8px;text-align:center">Score</th>
            </tr></thead>
            <tbody>{field_rows}</tbody>
          </table>
          {"" if not r.get("error") else f'<div style="color:#dc2626;margin-top:6px;font-size:.82rem">Error: {r["error"]}</div>'}
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Semantic Search Evaluation</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#F8FAFC;color:#1e293b;margin:0}}
  header{{background:linear-gradient(135deg,#3B82F6,#8B5CF6);color:#fff;padding:28px 48px}}
  main{{max-width:900px;margin:24px auto;padding:0 20px}}
  .stat-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}}
  .stat{{background:#fff;border-radius:12px;padding:16px;text-align:center;border:1px solid #e2e8f0}}
  .stat .num{{font-size:2rem;font-weight:800;color:#3B82F6}}
  .stat .lbl{{font-size:.78rem;color:#64748b;margin-top:4px}}
</style>
</head><body>
<header>
  <h1 style="margin:0;font-size:1.6rem">🔍 Semantic Search Evaluation</h1>
  <div style="opacity:.8;margin-top:4px;font-size:.9rem">
    {summary['n_total']} queries · Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}
  </div>
</header>
<main>
  <div class="stat-grid">
    <div class="stat"><div class="num">{summary['avg_f1']:.0%}</div><div class="lbl">Avg F1</div></div>
    <div class="stat"><div class="num">{summary['avg_precision']:.0%}</div><div class="lbl">Avg Precision</div></div>
    <div class="stat"><div class="num">{summary['avg_recall']:.0%}</div><div class="lbl">Avg Recall</div></div>
    <div class="stat"><div class="num">{summary['pct_perfect']:.0%}</div><div class="lbl">Perfect (F1≥0.8)</div></div>
  </div>
  {rows}
</main>
</body></html>"""

    output_path.write_text(html, encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Evaluate semantic search query parsing")
    parser.add_argument("--dry-run", action="store_true", help="Print queries only, no API calls")
    parser.add_argument("--limit",   type=int, default=None, help="Run first N test cases only")
    parser.add_argument("--output",  default="models/valuation/semantic_eval", help="Output dir")
    parser.add_argument("--delay",   type=float, default=0.3, help="Seconds between API calls")
    parser.add_argument("--blind",   action="store_true",
                        help="Run independent blind test set (BLIND_TEST_CASES) instead of author set")
    parser.add_argument("--annotator", default=None,
                        help="Filter blind test to one annotator: A, B, or C")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.blind:
        source = BLIND_TEST_CASES
        if args.annotator:
            source = [c for c in source if c.get("annotator") == args.annotator.upper()]
        cases = source[:args.limit] if args.limit else source
        print(f"\n  *** BLIND TEST SET{' (annotator '+args.annotator.upper()+')' if args.annotator else ''} ***")
    else:
        cases = TEST_CASES[:args.limit] if args.limit else TEST_CASES
    n     = len(cases)

    print(f"\n{'='*60}")
    print(f"  Semantic Search Evaluation — {n} queries")
    if args.dry_run:
        print("  DRY RUN — no API calls")
    print(f"{'='*60}\n")

    results = []
    total_tokens = 0

    for i, case in enumerate(cases, 1):
        query    = case["query"]
        expected = case["expected"]
        print(f"[{i:02d}/{n}] {query}")

        if args.dry_run:
            got    = {}
            error  = None
        else:
            try:
                got   = call_claude(query)
                error = None
                if i < n:
                    time.sleep(args.delay)
            except Exception as e:
                got   = {}
                error = str(e)
                print(f"       ✗ ERROR: {e}")

        scores = evaluate_case(expected, got)
        verdict = "✅" if scores["f1"] >= 0.8 else "⚠️" if scores["f1"] >= 0.5 else "❌"
        print(f"       {verdict} P={scores['precision']:.0%} R={scores['recall']:.0%} F1={scores['f1']:.0%}")
        if not args.dry_run:
            print(f"       Got: {json.dumps(got, ensure_ascii=False)}")

        results.append({
            "query":    query,
            "expected": expected,
            "got":      got,
            "scores":   scores,
            "error":    error,
        })

    # ── Summary ───────────────────────────────────────────────────────────────
    all_f1s  = [r["scores"]["f1"] for r in results]
    all_prec = [r["scores"]["precision"] for r in results]
    all_rec  = [r["scores"]["recall"]    for r in results]

    summary = {
        "n_total":      n,
        "avg_f1":       sum(all_f1s)  / n,
        "avg_precision":sum(all_prec) / n,
        "avg_recall":   sum(all_rec)  / n,
        "pct_perfect":  sum(1 for f in all_f1s if f >= 0.8) / n,
        "pct_partial":  sum(1 for f in all_f1s if 0.5 <= f < 0.8) / n,
        "pct_fail":     sum(1 for f in all_f1s if f < 0.5) / n,
        "generated_at": datetime.now().isoformat(),
        "test_set": "blind" if args.blind else "author",
    }

    # Per-annotator breakdown (blind set only)
    if args.blind:
        per_annotator = {}
        for ann in ("A", "B", "C"):
            ann_results = [r for r, c in zip(results, cases) if c.get("annotator") == ann]
            if ann_results:
                fs = [r["scores"]["f1"] for r in ann_results]
                per_annotator[ann] = {
                    "n": len(fs),
                    "avg_f1": round(sum(fs) / len(fs), 3),
                    "pct_perfect": round(sum(1 for f in fs if f >= 0.8) / len(fs), 3),
                }
        summary["per_annotator"] = per_annotator

    bar = "=" * 60
    print(f"\n{bar}")
    print(f"  RESULTS ({n} queries) — {'BLIND SET' if args.blind else 'AUTHOR SET'}")
    print(bar)
    print(f"  Avg F1:        {summary['avg_f1']:.1%}")
    print(f"  Avg Precision: {summary['avg_precision']:.1%}")
    print(f"  Avg Recall:    {summary['avg_recall']:.1%}")
    print(f"  Perfect (≥0.8):{summary['pct_perfect']:.1%}  ({int(summary['pct_perfect']*n)}/{n})")
    print(f"  Partial (≥0.5):{summary['pct_partial']:.1%}  ({int(summary['pct_partial']*n)}/{n})")
    print(f"  Fail (<0.5):   {summary['pct_fail']:.1%}  ({int(summary['pct_fail']*n)}/{n})")
    if args.blind and "per_annotator" in summary:
        print(f"\n  Per-annotator breakdown:")
        for ann, stats in summary["per_annotator"].items():
            print(f"    Annotator {ann}: n={stats['n']}  avg_F1={stats['avg_f1']:.1%}  "
                  f"perfect={stats['pct_perfect']:.0%}")

    # Worst cases
    worst = sorted(results, key=lambda r: r["scores"]["f1"])[:5]
    print(f"\n  Worst performing queries:")
    for r in worst:
        print(f"    F1={r['scores']['f1']:.0%}  {r['query'][:70]}")
    print(bar)

    # Save JSON
    json_path = output_dir / "eval_results.json"
    json_path.write_text(
        json.dumps({"summary": summary, "results": results}, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    print(f"\n  JSON saved: {json_path}")

    # Save HTML
    if not args.dry_run:
        html_path = output_dir / "eval_report.html"
        render_html(results, summary, html_path)
        print(f"  HTML saved: {html_path}")
        print(f"  Open: file://{html_path.resolve()}")

    print()
    return summary


if __name__ == "__main__":
    main()
