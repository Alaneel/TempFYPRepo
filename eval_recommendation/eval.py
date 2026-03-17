"""
Offline Recommendation Evaluation — NDCG@5 / NDCG@10
=====================================================
Evaluates the hybrid content-based + valuation-grounded recommendation engine
using 10 synthetic user profiles derived directly from the live listings database.

Methodology
-----------
For each synthetic profile:
  1. Randomly sample 5 "seed" listings that share the same property_type,
     district, and buy_rent (simulating a focused user preference).
  2. Designate 10 additional listings from the same segment as the
     "ground truth relevant set" (held-out positives).
  3. Run the scoring function (_score_candidate) on a pool of 200 candidates.
  4. Compute NDCG@5 and NDCG@10 against the ground truth set.
  5. Also report: precision@5, precision@10, type-match rate, district-match rate.

NDCG formula (binary relevance, base-2 log):
    DCG@k  = sum_{i=1}^{k}  rel_i / log2(i+1)
    IDCG@k = sum_{i=1}^{min(k,|R|)}  1 / log2(i+1)
    NDCG@k = DCG@k / IDCG@k

Usage
-----
    cd /Users/alanwang/PycharmProjects/PythonProject/backend
    python ../poc_recommendation/eval.py
"""

import sys
import os
import json
import math
import random
import asyncio
from pathlib import Path
from collections import defaultdict

# ── Make sure the backend package is importable ────────────────────────────────
BACKEND_DIR = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models.listing import Listing
from app.routers.recommendations import (
    _score_candidate,
    _build_preference_profile,
)
from app.models.favourite import UserFavourite

random.seed(42)

# ── Config ─────────────────────────────────────────────────────────────────────
N_PROFILES       = 10
N_SEED_PER_PROFILE  = 5    # listings used to build the preference profile
N_RELEVANT       = 10    # ground-truth relevant listings (held-out)
CANDIDATE_POOL   = 300   # candidates scored per profile
K_VALUES         = [5, 10]

OUTPUT_PATH = Path(__file__).parent / "eval_results.json"


# ── NDCG helpers ───────────────────────────────────────────────────────────────

def dcg_at_k(ranked_ids: list[int], relevant_ids: set[int], k: int) -> float:
    dcg = 0.0
    for i, lid in enumerate(ranked_ids[:k], start=1):
        if lid in relevant_ids:
            dcg += 1.0 / math.log2(i + 1)
    return dcg


def ndcg_at_k(ranked_ids: list[int], relevant_ids: set[int], k: int) -> float:
    actual_dcg = dcg_at_k(ranked_ids, relevant_ids, k)
    ideal_hits = min(k, len(relevant_ids))
    ideal_dcg  = sum(1.0 / math.log2(i + 1) for i in range(1, ideal_hits + 1))
    return actual_dcg / ideal_dcg if ideal_dcg > 0 else 0.0


def precision_at_k(ranked_ids: list[int], relevant_ids: set[int], k: int) -> float:
    hits = sum(1 for lid in ranked_ids[:k] if lid in relevant_ids)
    return hits / k


# ── Main evaluation ────────────────────────────────────────────────────────────

async def run_evaluation():
    print("Loading listings from database …")
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Listing)
            .options(selectinload(Listing.agent), selectinload(Listing.condo))
            .where(
                Listing.is_active == True,
                Listing.property_type.isnot(None),
                Listing.district.isnot(None),
                Listing.price.isnot(None),
                Listing.beds.isnot(None),
                Listing.buy_rent.isnot(None),
            )
        )
        all_listings: list[Listing] = result.scalars().all()

    print(f"  Loaded {len(all_listings):,} active listings with full features.")

    # Group listings by (property_type, district, buy_rent) segment
    segments: dict[tuple, list[Listing]] = defaultdict(list)
    for l in all_listings:
        seg = (l.property_type, l.district, l.buy_rent)
        segments[seg].append(l)

    # Keep only segments with enough listings for seed + relevant + candidates
    min_size = N_SEED_PER_PROFILE + N_RELEVANT + CANDIDATE_POOL // 3
    viable = [(seg, listings) for seg, listings in segments.items()
              if len(listings) >= min_size]
    print(f"  Viable segments (≥{min_size} listings): {len(viable)}")

    if len(viable) < N_PROFILES:
        # Relax: take top-N by size
        viable.sort(key=lambda x: len(x[1]), reverse=True)
        viable = viable[:N_PROFILES]
        print(f"  Relaxed to top-{N_PROFILES} segments by size.")

    # Sample N_PROFILES profiles from distinct segments
    chosen_segments = random.sample(viable, min(N_PROFILES, len(viable)))

    profile_results = []

    for profile_idx, (seg, seg_listings) in enumerate(chosen_segments, 1):
        prop_type, district, buy_rent = seg
        shuffled = seg_listings.copy()
        random.shuffle(shuffled)

        seed_listings    = shuffled[:N_SEED_PER_PROFILE]
        relevant_listings = shuffled[N_SEED_PER_PROFILE:N_SEED_PER_PROFILE + N_RELEVANT]
        relevant_ids      = {l.id for l in relevant_listings}

        # Build candidate pool: relevant + random from other segments (noise)
        noise_pool = [l for l in all_listings
                      if l.id not in {s.id for s in seed_listings}
                      and l.id not in relevant_ids]
        noise_sample = random.sample(noise_pool, min(CANDIDATE_POOL - N_RELEVANT, len(noise_pool)))
        candidates = relevant_listings + noise_sample
        random.shuffle(candidates)

        # Build synthetic UserFavourite-like objects for _build_preference_profile
        class _FakeFav:
            def __init__(self, l):
                self.listing = l
                self.listing_id = l.id

        fake_favs = [_FakeFav(l) for l in seed_listings]
        profile = _build_preference_profile(fake_favs)
        profile["favourited_ids"] = {l.id for l in seed_listings}

        # Score candidates
        scored = []
        for c in candidates:
            if c.id in profile["favourited_ids"]:
                continue
            score, reasons, _ = _score_candidate(c, profile)
            scored.append((score, c.id))

        scored.sort(reverse=True)
        ranked_ids = [lid for _, lid in scored]

        # Compute metrics
        metrics = {}
        for k in K_VALUES:
            metrics[f"ndcg@{k}"]      = round(ndcg_at_k(ranked_ids, relevant_ids, k), 4)
            metrics[f"precision@{k}"] = round(precision_at_k(ranked_ids, relevant_ids, k), 4)

        # Type-match and district-match rate in top-5
        top5_listings = [l for l in candidates if l.id in set(ranked_ids[:5])]
        type_match   = sum(1 for l in top5_listings if l.property_type == prop_type) / max(len(top5_listings), 1)
        district_match = sum(1 for l in top5_listings if l.district == district) / max(len(top5_listings), 1)

        profile_results.append({
            "profile": profile_idx,
            "segment": f"{prop_type} | D{district} | {buy_rent}",
            "seed_count": len(seed_listings),
            "relevant_count": len(relevant_ids),
            "candidate_pool": len(candidates),
            **metrics,
            "type_match_top5":    round(type_match, 4),
            "district_match_top5": round(district_match, 4),
        })

        print(f"  Profile {profile_idx:2d} [{prop_type} D{district} {buy_rent}]: "
              f"NDCG@5={metrics['ndcg@5']:.3f}  NDCG@10={metrics['ndcg@10']:.3f}  "
              f"P@5={metrics['precision@5']:.2f}  P@10={metrics['precision@10']:.2f}")

    # Aggregate
    agg = {}
    for k in K_VALUES:
        agg[f"mean_ndcg@{k}"]      = round(sum(r[f"ndcg@{k}"]      for r in profile_results) / len(profile_results), 4)
        agg[f"mean_precision@{k}"] = round(sum(r[f"precision@{k}"] for r in profile_results) / len(profile_results), 4)
    agg["mean_type_match_top5"]     = round(sum(r["type_match_top5"]     for r in profile_results) / len(profile_results), 4)
    agg["mean_district_match_top5"] = round(sum(r["district_match_top5"] for r in profile_results) / len(profile_results), 4)

    output = {
        "n_profiles": len(profile_results),
        "aggregate": agg,
        "profiles": profile_results,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2))

    print("\n" + "="*60)
    print("AGGREGATE RESULTS")
    print("="*60)
    for k in K_VALUES:
        print(f"  Mean NDCG@{k}:       {agg[f'mean_ndcg@{k}']:.4f}")
        print(f"  Mean Precision@{k}: {agg[f'mean_precision@{k}']:.4f}")
    print(f"  Mean Type-Match@5:     {agg['mean_type_match_top5']:.4f}")
    print(f"  Mean District-Match@5: {agg['mean_district_match_top5']:.4f}")
    print(f"\nResults saved → {OUTPUT_PATH}")


if __name__ == "__main__":
    asyncio.run(run_evaluation())
