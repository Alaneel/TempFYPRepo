"""
Deduplication Algorithm Evaluation
====================================
Measures the quality of the address+beds+baths+sqft dedup key used in aggregate.py.

Metrics:
  - False Negatives (missed duplicates): pairs of listings in DB that look
    like the same property but weren't collapsed (same beds/sqft/buy_rent,
    very similar price, very similar address).
  - False Positives (over-dedup): we can't directly measure from post-dedup
    data, but we can detect suspicious key collisions where sqft=0/null were
    bucketed together, or where address normalization was too aggressive.
  - Key quality stats: uniqueness rate, key component coverage, etc.
  - Source overlap analysis: what % of listings appear in multiple sources.

Usage:
    python pipeline/eval_dedup.py
    python pipeline/eval_dedup.py --limit 20000   # faster sample
"""

import argparse
import os
import re
import sys
import json
from pathlib import Path
from datetime import datetime

os.environ.setdefault("PYTHONUTF8", "1")

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "real_estate_app")


# ──────────────────────────────────────────────────────────────────────────────
# Normalization (mirrors aggregate.py logic)
# ──────────────────────────────────────────────────────────────────────────────
def _clean_beds(val):
    try:
        v = float(val)
        return int(round(v)) if not np.isnan(v) else 0
    except (TypeError, ValueError):
        try:
            m = re.search(r"\d+", str(val))
            return int(m.group()) if m else 0
        except Exception:
            return 0


def _clean_sqft(val):
    try:
        v = float(val)
        return int(round(v)) if not np.isnan(v) else 0
    except (TypeError, ValueError):
        try:
            m = re.search(r"[\d,]+", str(val).replace(",", ""))
            return int(m.group()) if m else 0
        except Exception:
            return 0


def _norm_addr(val):
    return re.sub(r"[^a-z0-9]", "", str(val).lower())


def build_dedup_key(row):
    addr = _norm_addr(row.get("street_name") or row.get("address") or "")
    beds  = _clean_beds(row.get("beds"))
    baths = _clean_beds(row.get("baths"))
    sqft  = _clean_sqft(row.get("sqft"))
    return f"{addr}_{beds}_{baths}_{sqft}"


# ──────────────────────────────────────────────────────────────────────────────
# Load data
# ──────────────────────────────────────────────────────────────────────────────
def load_listings(limit=None):
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(url)
    q = """
        SELECT id, source, property_type, buy_rent,
               beds, baths, sqft, price,
               street_name, title
        FROM listings
        WHERE price > 0
    """
    if limit:
        q += f" ORDER BY id LIMIT {limit}"
    df = pd.read_sql(q, engine)
    print(f"Loaded {len(df):,} listings from DB.")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Analysis functions
# ──────────────────────────────────────────────────────────────────────────────

def analyze_key_coverage(df):
    """How complete are the dedup key components?"""
    print("\n=== Key Component Coverage ===")
    total = len(df)
    stats = {}
    for col in ["street_name", "beds", "baths", "sqft"]:
        valid = df[col].notna() & (df[col] != "") & (df[col] != 0)
        pct = valid.sum() / total * 100
        stats[col] = {"valid": int(valid.sum()), "pct": round(pct, 1)}
        print(f"  {col:15s}: {valid.sum():>6,} / {total:,}  ({pct:.1f}%)")

    # How many have addr=empty after normalization?
    addr_norm = df["street_name"].fillna("").str.lower().str.replace(r"[^a-z0-9]", "", regex=True)
    empty_addr = (addr_norm == "").sum()
    print(f"\n  Listings with empty normalized address: {empty_addr:,} ({empty_addr/total*100:.1f}%)")
    if empty_addr > 0:
        sample = df[addr_norm == ""][["source", "property_type", "buy_rent", "price"]].head(5)
        print("  Sample:")
        print(sample.to_string(index=False))

    return stats


def analyze_false_negatives(df):
    """
    Find suspected missed duplicates (false negatives):
    Pairs with SAME (beds, baths, sqft, buy_rent, property_type)
    and price within 2% and similar title keywords.

    Uses groupby instead of O(n²) self-join.
    """
    print("\n=== False Negative Analysis (Missed Duplicates) ===")

    df = df.copy()
    df["beds_n"]  = df["beds"].apply(_clean_beds)
    df["baths_n"] = df["baths"].apply(_clean_beds)
    df["sqft_n"]  = df["sqft"].apply(_clean_sqft)

    # Round sqft to nearest 10 (scraper rounding)
    df["sqft_bucket"] = (df["sqft_n"] / 10).round() * 10

    # Group key
    df["group_key"] = (
        df["property_type"].fillna("") + "_" +
        df["buy_rent"].fillna("") + "_" +
        df["beds_n"].astype(str) + "_" +
        df["baths_n"].astype(str) + "_" +
        df["sqft_bucket"].astype(str)
    )

    suspicious = []
    groups = df.groupby("group_key")
    for key, grp in groups:
        if len(grp) < 2:
            continue

        # Within group: find pairs where prices are very close
        prices = grp["price"].values
        ids    = grp["id"].values
        srcs   = grp["source"].values
        addrs  = grp["street_name"].fillna("").values

        for i in range(len(grp)):
            for j in range(i + 1, len(grp)):
                p_i, p_j = prices[i], prices[j]
                if p_i == 0 or p_j == 0:
                    continue
                diff_pct = abs(p_i - p_j) / max(p_i, p_j)
                if diff_pct > 0.03:      # more than 3% price diff → probably distinct listings
                    continue

                # Check address similarity
                a_i = _norm_addr(addrs[i])
                a_j = _norm_addr(addrs[j])
                if a_i and a_j and a_i != a_j:
                    # Different addresses — might be a legitimate coincidence
                    # Still flag if price diff is tiny (<1%)
                    if diff_pct > 0.005:
                        continue

                suspicious.append({
                    "id_a":    int(ids[i]),
                    "id_b":    int(ids[j]),
                    "source_a": srcs[i],
                    "source_b": srcs[j],
                    "beds":    grp["beds_n"].iloc[i],
                    "sqft":    grp["sqft_n"].iloc[i],
                    "buy_rent": key.split("_")[1] + "-" + key.split("_")[2] if len(key.split("_")) > 2 else "",
                    "price_a": round(p_i),
                    "price_b": round(p_j),
                    "price_diff_pct": round(diff_pct * 100, 2),
                    "addr_a":  addrs[i],
                    "addr_b":  addrs[j],
                    "group_key": key,
                })
                if len(suspicious) > 10000:  # cap
                    break
            if len(suspicious) > 10000:
                break

    n_suspected = len(suspicious)
    total = len(df)
    print(f"  Suspicious missed-duplicate pairs: {n_suspected:,}")
    print(f"  Affected listings estimate: ~{min(n_suspected * 2, total):,}")
    pct = min(n_suspected * 2, total) / total * 100
    print(f"  False negative rate (estimate): {pct:.2f}%")

    # Same-source vs cross-source
    same_src = [s for s in suspicious if s["source_a"] == s["source_b"]]
    cross_src = [s for s in suspicious if s["source_a"] != s["source_b"]]
    print(f"\n  Same-source pairs  : {len(same_src):,}  (scraper re-crawl duplicates not caught)")
    print(f"  Cross-source pairs : {len(cross_src):,}  (expected — different platforms, same listing)")

    if suspicious[:5]:
        print("\n  Sample suspected missed duplicates:")
        for s in suspicious[:5]:
            print(f"    ID {s['id_a']} ({s['source_a']}) vs {s['id_b']} ({s['source_b']})"
                  f"  beds={s['beds']} sqft={s['sqft']}"
                  f"  price Δ={s['price_diff_pct']}%"
                  f"  addr: '{s['addr_a'][:30]}' / '{s['addr_b'][:30]}'")
    return suspicious


def analyze_key_collisions(df):
    """
    False positives: cases where the dedup key is TOO broad.
    Indicator: groups where the key is "0_0_0" or address is empty,
    so completely unrelated listings collapsed together.
    """
    print("\n=== False Positive Risk (Over-Deduplication) ===")

    df = df.copy()
    df["dedup_key"] = df.apply(build_dedup_key, axis=1)

    # Identify high-risk keys (address component is empty / sqft=0)
    df["addr_part"] = df["street_name"].fillna("").str.lower().str.replace(r"[^a-z0-9]", "", regex=True)
    df["sqft_n"]    = df["sqft"].apply(_clean_sqft)

    dangerous = df[(df["addr_part"] == "") | (df["sqft_n"] == 0)]
    print(f"  Listings with empty addr OR sqft=0 (collision-prone): {len(dangerous):,}  "
          f"({len(dangerous)/len(df)*100:.1f}%)")

    # Key with too many different prices (collision indicator)
    key_groups = df.groupby("dedup_key").agg(
        n=("id", "count"),
        n_sources=("source", "nunique"),
        price_std=("price", "std"),
        price_min=("price", "min"),
        price_max=("price", "max"),
    ).reset_index()
    key_groups = key_groups[key_groups["n"] > 1]

    # Suspicious: same key, wildly different prices (>50% apart)
    key_groups["price_spread"] = (key_groups["price_max"] - key_groups["price_min"]) / key_groups["price_max"].clip(lower=1)
    risky = key_groups[key_groups["price_spread"] > 0.5]

    print(f"  Dedup keys with 2+ listings (post-dedup, should be 0): "
          f"{len(key_groups):,}")
    print(f"    — with price spread >50% (likely false positives in original): "
          f"{len(risky):,}")

    if len(risky) > 0:
        print("\n  Sample high-risk keys:")
        sample = risky.sort_values("price_spread", ascending=False).head(5)
        for _, row in sample.iterrows():
            key_listings = df[df["dedup_key"] == row["dedup_key"]][
                ["id", "source", "price", "beds", "sqft", "street_name"]
            ].head(3)
            print(f"\n    Key: {row['dedup_key'][:60]}  (n={row['n']}, spread={row['price_spread']:.0%})")
            print("    " + key_listings.to_string(index=False).replace("\n", "\n    "))

    return {
        "dangerous_listings": len(dangerous),
        "keys_with_duplicates_post_dedup": len(key_groups),
        "high_spread_keys": len(risky),
    }


def analyze_cross_source_overlap(df):
    """How well does dedup handle the same listing from multiple sources?"""
    print("\n=== Cross-Source Overlap (Expected Duplicates) ===")

    df = df.copy()
    df["dedup_key"] = df.apply(build_dedup_key, axis=1)
    df["addr_part"] = df["street_name"].fillna("").str.lower().str.replace(r"[^a-z0-9]", "", regex=True)

    by_source = df.groupby("source").size()
    print(f"  Source distribution:")
    for src, n in by_source.items():
        print(f"    {src:15s}: {n:>6,}")

    # Check if same listing (by key) exists in multiple sources in the cache CSV
    # Since post-ingest DB has already deduped, check what's left
    key_sources = df.groupby("dedup_key")["source"].agg(list)
    multi_source = key_sources[key_sources.map(len) > 1]
    print(f"\n  Keys appearing in multiple sources in DB (should be ~0 if dedup worked): {len(multi_source):,}")
    if len(multi_source) > 0:
        print("  Sample:")
        for key, srcs in list(multi_source.items())[:5]:
            print(f"    {key[:50]}  → {srcs}")

    return {"multi_source_leakage": len(multi_source)}


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Max rows to load (for speed)")
    parser.add_argument("--output", default="models/valuation/dedup_eval", help="Output dir")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*60)
    print("  Deduplication Algorithm Evaluation")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    df = load_listings(limit=args.limit)

    cov_stats   = analyze_key_coverage(df)
    fn_pairs    = analyze_false_negatives(df)
    fp_stats    = analyze_key_collisions(df)
    xs_stats    = analyze_cross_source_overlap(df)

    # ── Summary ──────────────────────────────────────────────────────────────
    n = len(df)
    same_src_fn  = len([s for s in fn_pairs if s["source_a"] == s["source_b"]])
    cross_src_fn = len([s for s in fn_pairs if s["source_a"] != s["source_b"]])

    print("\n" + "="*60)
    print("  SUMMARY")
    print("="*60)
    print(f"  Total listings in DB         : {n:,}")
    print(f"  Key coverage (addr+sqft)     : {cov_stats.get('sqft', {}).get('pct','?')}% sqft, "
          f"{(df['street_name'].notna().sum()/n*100):.1f}% addr")
    print(f"  Missed duplicates (FN pairs) : {len(fn_pairs):,}  (~{same_src_fn} same-src, ~{cross_src_fn} cross-src)")
    print(f"  High-spread key collisions   : {fp_stats['high_spread_keys']:,}")
    print(f"  Multi-source leakage in DB   : {xs_stats['multi_source_leakage']:,}")

    # Grade the algorithm
    fn_rate = min(len(fn_pairs) * 2, n) / n
    if fn_rate < 0.02 and fp_stats["high_spread_keys"] < 100:
        grade = "✅ GOOD — dedup is working well"
    elif fn_rate < 0.05 or fp_stats["high_spread_keys"] < 500:
        grade = "⚠️  ACCEPTABLE — minor issues, acceptable for FYP"
    else:
        grade = "❌ NEEDS IMPROVEMENT"

    print(f"\n  Overall grade: {grade}")
    print(f"  FN rate estimate: {fn_rate*100:.2f}%")
    print("="*60)

    summary = {
        "total_listings": n,
        "fn_pairs": len(fn_pairs),
        "same_source_fn": same_src_fn,
        "cross_source_fn": cross_src_fn,
        "fn_rate_pct": round(fn_rate * 100, 2),
        "high_spread_key_collisions": fp_stats["high_spread_keys"],
        "multi_source_leakage": xs_stats["multi_source_leakage"],
        "grade": grade,
        "generated_at": datetime.now().isoformat(),
    }
    out_path = output_dir / "dedup_eval.json"
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\n  Report saved: {out_path}")

if __name__ == "__main__":
    main()
