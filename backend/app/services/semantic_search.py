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

def get_client():
    """Public accessor for the shared Anthropic client."""
    return _get_client()


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
  "min_price":     number,          // minimum price in SGD (for rent: monthly, for sale: total)
  "max_price":     number,          // maximum price in SGD
  "beds":          number,          // minimum number of bedrooms
  "baths":         number,          // minimum number of bathrooms
  "property_type": string,          // one of: "Condominium", "HDB", "Landed"
  "buy_rent":      string,          // "property-for-sale" or "property-for-rent"
  "districts":     array of number, // one or more Singapore district numbers (1-28)
  "tenure":        string,          // "Freehold" or "Leasehold 99" or "Leasehold 999"
  "q":             string           // project name only (e.g. "The Interlace"), NOT a location
}

Singapore district reference (include nearby districts when user says "near"):
- D1:  Raffles Place, Cecil, Marina, People's Park
- D2:  Anson, Tanjong Pagar
- D3:  Queenstown, Tiong Bahru
- D4:  Harbourfront, Telok Blangah, Sentosa
- D5:  Buona Vista, West Coast, Clementi, Dover
- D6:  City Hall, High Street
- D7:  Middle Road, Beach Road, Bugis
- D8:  Little India, Farrer Park
- D9:  Orchard, River Valley, Cairnhill
- D10: Bukit Timah, Holland Village, Tanglin, Balmoral
- D11: Newton, Novena, Balestier, Thomson
- D12: Toa Payoh, Serangoon, Braddell
- D13: Macpherson, Potong Pasir, Geylang
- D14: Eunos, Paya Lebar, Geylang
- D15: Katong, Joo Chiat, Amber Road, Marine Parade
- D16: Bedok, Upper East Coast, Eastwood
- D17: Loyang, Changi
- D18: Tampines, Pasir Ris
- D19: Punggol, Sengkang, Hougang, Buangkok
- D20: Bishan, Ang Mo Kio, Thomson
- D21: Upper Bukit Timah, Ulu Pandan, Clementi Park
- D22: Jurong, Boon Lay, Tuas
- D23: Bukit Panjang, Choa Chu Kang, Hillview
- D24: Lim Chu Kang, Tengah
- D25: Kranji, Woodgrove, Woodlands
- D26: Upper Thomson, Springleaf, Yio Chu Kang
- D27: Yishun, Sembawang
- D28: Seletar, Yio Chu Kang

Location rules for "districts" field:
- Exact area (e.g. "in Orchard", "in Tampines") → single district [9], [18]
- "near X" or "around X" → primary district PLUS immediately adjacent districts
  Examples:
  * "near Orchard MRT"   → [9, 10, 11]   (Orchard + Bukit Timah + Newton)
  * "near Tampines MRT"  → [18, 19, 16]  (Tampines + Punggol/Sengkang + Bedok)
  * "near Bishan MRT"    → [20, 12, 11]  (Bishan + Toa Payoh + Novena)
  * "near Jurong East"   → [22, 5, 23]   (Jurong + Clementi + Bukit Panjang)
  * "near CBD / Raffles" → [1, 2, 6]
  * "near Novena"        → [11, 12, 20]
  * "near Punggol"       → [19]
  * "near Bedok"         → [16, 15, 18]
  * "near Clementi"      → [5, 21, 22]
  * "near Woodlands"     → [25, 27, 26]
  * "near Yishun"        → [27, 26, 25]
  * "near Bukit Timah"   → [10, 21, 11]
  * Any unknown MRT/area → use your knowledge of Singapore geography, include 2-3 nearby districts

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
        "q":             "query",        # ListingService uses "query" for free-text
        "tenure":        "tenure",
    }

    result = {}
    for src, dst in field_map.items():
        if src in filters and filters[src] is not None:
            result[dst] = filters[src]

    # Handle districts array (new multi-district support for "near X" queries)
    districts = filters.get("districts")
    if isinstance(districts, list) and districts:
        valid = [int(d) for d in districts if isinstance(d, (int, float)) and 1 <= d <= 28]
        if valid:
            result["districts"] = valid
    # Backwards compat: scalar "district" field
    elif filters.get("district") is not None:
        result["districts"] = [int(filters["district"])]

    # Remove old scalar district key if present
    result.pop("district", None)
    # Remove any stale bbox fields
    for k in ("min_lat", "max_lat", "min_lng", "max_lng", "_location_label"):
        result.pop(k, None)

    print(f"[semantic_search] parsed: {result}")
    return result


def generate_fallback_explanation(original_query: str, relaxed_filters: dict, result_count: int) -> str:
    """
    When AI search found 0 results with original filters and had to relax them,
    generate a natural language explanation of why and what was changed.

    Args:
        original_query:  The user's original search string.
        relaxed_filters: The filters actually used (after relaxation).
        result_count:    Number of results found with relaxed filters.

    Returns:
        A short, friendly explanation string (1–2 sentences).
    """
    # Build a readable summary of what filters remain
    remaining = []
    if relaxed_filters.get("beds"):
        remaining.append(f"{relaxed_filters['beds']} bedrooms")
    if relaxed_filters.get("property_type"):
        remaining.append(relaxed_filters["property_type"])
    if relaxed_filters.get("buy_rent"):
        remaining.append("for rent" if "rent" in relaxed_filters["buy_rent"] else "for sale")
    if relaxed_filters.get("districts"):
        remaining.append(f"District {'/'.join(str(d) for d in relaxed_filters['districts'])}")
    elif relaxed_filters.get("district"):
        remaining.append(f"District {relaxed_filters['district']}")

    removed = []
    if "max_price" not in relaxed_filters and "min_price" not in relaxed_filters:
        removed.append("price")
    if "tenure" not in relaxed_filters:
        removed.append("tenure")
    if "districts" not in relaxed_filters and "district" not in relaxed_filters:
        removed.append("location")

    prompt = (
        f"A user searched for Singapore property: \"{original_query}\"\n"
        f"No exact matches were found. The system relaxed these filters: {', '.join(removed)}.\n"
        f"Now showing {result_count} results matching: {', '.join(remaining) if remaining else 'all listings'}.\n\n"
        "Write ONE short, friendly sentence (max 25 words) explaining why no exact matches were found "
        "and what the user is now seeing. Be specific about the likely reason (e.g. market price vs budget). "
        "Do not start with 'I'. No markdown."
    )

    try:
        response = _get_client().messages.create(
            model="claude-haiku-4-5",   # fast & cheap for short explanations
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except Exception:
        # Graceful fallback if Claude call fails
        removed_str = " & ".join(removed) if removed else "some"
        return f"No exact matches found — {removed_str} filter{'s' if len(removed) > 1 else ''} relaxed to show {result_count} nearby results."


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