"""
Semantic Search Service
=======================
Uses Claude API to parse natural language property queries
into structured filter parameters for the listings API.

Setup:
    pip install anthropic
    export ANTHROPIC_API_KEY="sk-ant-..."

Place this file at: backend/app/services/semantic_search.py
"""

import json
import re
import os
import anthropic
from typing import Optional
from app.config import settings

# ── Claude client (lazy init so missing key fails at call-time, not import) ───
_client = None

def _get_client():
    global _client
    if _client is None:
        api_key = settings.ANTHROPIC_API_KEY or os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set. Add it to your .env file:\n"
                "  ANTHROPIC_API_KEY=sk-ant-..."
            )
        _client = anthropic.Anthropic(api_key=api_key)
    return _client

# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a Singapore real estate search assistant.

Your job is to parse a natural language property search query and extract structured filter parameters.

Output ONLY a valid JSON object with these fields (omit fields you cannot determine):
{
  "min_price":     number,   // minimum price in SGD (for rent: monthly, for sale: total)
  "max_price":     number,   // maximum price in SGD
  "beds":          number,   // minimum number of bedrooms
  "baths":         number,   // minimum number of bathrooms
  "property_type": string,   // one of: "Condominium", "HDB", "Landed"
  "buy_rent":      string,   // "property-for-sale" or "property-for-rent"
  "district":      number,   // Singapore district number (1-28)
  "tenure":        string,   // "Freehold" or "Leasehold 99" or "Leasehold 999"
  "q":             string    // free-text for location/project name not captured above
}

Singapore district mapping (key areas):
- D1-D4:  CBD, Marina Bay, Harbourfront, Sentosa
- D5:     Buona Vista, Clementi, Dover
- D9:     Orchard, River Valley
- D10:    Bukit Timah, Holland Village
- D11:    Newton, Novena, Balestier
- D15:    Katong, Joo Chiat, Marine Parade
- D18:    Tampines, Pasir Ris
- D19:    Punggol, Sengkang, Hougang
- D20:    Bishan, Ang Mo Kio
- D23:    Bukit Panjang, Choa Chu Kang

Price interpretation rules:
- "k" = thousands (800k = 800000)
- "m" or "million" = millions (1.2m = 1200000)
- For rent: if price < 20000, treat as monthly SGD rent
- For sale: if price > 100000, treat as total purchase price
- "3500/mo" or "3500 per month" → max_price: 3500 (rent context)

Common abbreviations:
- "BR", "rm", "room", "bedroom" → beds
- "condo", "apartment" → Condominium
- "fh" → Freehold
- "99yr", "99-year", "leasehold" → Leasehold 99
- "nr MRT", "near MRT", project names → put in q field

Output ONLY the JSON object. No explanation, no markdown fences, no extra text."""


def parse_nl_query(query: str) -> dict:
    """
    Parse a natural language property search query into structured filter parameters.

    Args:
        query: Natural language search string from the user.

    Returns:
        Dict of filter parameters compatible with ListingService.get_listings().
        Keys: min_price, max_price, beds, property_type, buy_rent,
              district, query (free-text), tenure.

    Example:
        parse_nl_query("3BR condo Tampines budget 1.2m freehold")
        → {
            "beds": 3,
            "property_type": "Condominium",
            "max_price": 1200000,
            "tenure": "Freehold",
            "buy_rent": "property-for-sale",
            "district": 18
          }
    """
    if not query or not query.strip():
        return {}

    response = _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": query.strip()}]
    )

    raw = response.content[0].text.strip()

    # Strip markdown fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    filters = json.loads(raw)

    # Map Claude's output fields → ListingService parameter names
    field_map = {
        "min_price":     "min_price",
        "max_price":     "max_price",
        "beds":          "beds",
        "property_type": "property_type",
        "buy_rent":      "buy_rent",
        "district":      "district",
        "q":             "query",        # ListingService uses "query" for free-text
        "tenure":        "tenure",
    }

    result = {}
    for src, dst in field_map.items():
        if src in filters and filters[src] is not None:
            result[dst] = filters[src]

    print(f"[semantic_search] parsed: {result}")
    return result


# ── Standalone test ───────────────────────────────────────────────────────────
TEST_QUERIES = [
    "3 bedroom condo near Tampines MRT, budget 1.2 million, freehold",
    "cheap HDB for rent in Bishan under 2500 per month",
    "2BR apartment Orchard area for sale around 1.8m",
    "4 room HDB Punggol sale below 600k",
    "studio condo rent near CBD, max 3000/mo",
    "landed property Bukit Timah freehold 5 bedrooms",
    "executive condo EC Sengkang 3BR sale",
    "near good schools, 4BR condo district 10, 2-3 million",
]

if __name__ == "__main__":
    print("=" * 60)
    print("Semantic Search Parser — Test Run")
    print("=" * 60)

    passed = 0
    for i, query in enumerate(TEST_QUERIES, 1):
        print(f"\n[{i}/{len(TEST_QUERIES)}] {query}")
        try:
            result = parse_nl_query(query)
            print(f"  → {json.dumps(result, ensure_ascii=False)}")
            passed += 1
        except Exception as e:
            print(f"  ✗ ERROR: {e}")

    print(f"\n{'='*60}")
    print(f"Results: {passed}/{len(TEST_QUERIES)} passed")