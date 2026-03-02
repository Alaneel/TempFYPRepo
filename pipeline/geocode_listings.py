"""
Geocode listings using Singapore OneMap API
============================================
策略：
1. 优先用 address 查（更精确）
2. address 查不到或为空时，用 title 查
3. 结果写回 listings.latitude / longitude
4. 已有坐标的跳过
5. 支持断点续跑（--resume）

用法：
    python pipeline/geocode_listings.py              # 跑全部
    python pipeline/geocode_listings.py --limit 500  # 只跑500条测试
    python pipeline/geocode_listings.py --resume     # 跳过已处理的（从上次中断继续）
"""

import os
import sys
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import execute_batch
from typing import Optional, Tuple

# ── DB 配置 ────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "real_estate_app")
DB_USER = os.getenv("DB_USER", "alanwang")
DB_PASS = os.getenv("DB_PASS", "")

ONEMAP_SEARCH = "https://www.onemap.gov.sg/api/common/elastic/search"

# 每批提交数量 & 请求间隔（避免被限速）
BATCH_SIZE = 100
SLEEP_BETWEEN_REQUESTS = 0.15  # 秒


def onemap_search(query: str) -> Optional[Tuple[float, float]]:
    """调用 OneMap API，返回 (lat, lng) 或 None"""
    try:
        resp = requests.get(
            ONEMAP_SEARCH,
            params={"searchVal": query, "returnGeom": "Y", "getAddrDetails": "N", "pageNum": 1},
            timeout=8,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data.get("results", [])
        if not results:
            return None
        first = results[0]
        lat = float(first.get("LATITUDE", 0))
        lng = float(first.get("LONGITUDE", 0))
        if lat == 0 and lng == 0:
            return None
        return lat, lng
    except Exception:
        return None


def geocode_one(title: str, address: str) -> Optional[Tuple[float, float]]:
    """先查 address，失败再查 title"""
    # 1. address（去掉末尾重复内容，如 "609 Elias Road" 有时更精确）
    if address and address.strip():
        result = onemap_search(address.strip())
        if result:
            return result

    # 2. title
    if title and title.strip():
        result = onemap_search(title.strip())
        if result:
            return result

    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少条（测试用）")
    parser.add_argument("--resume", action="store_true", help="跳过已有坐标的记录（默认行为，保留此参数兼容）")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()

    # 取所有没有坐标的 listings
    query = "SELECT id, title, address FROM listings WHERE latitude IS NULL OR longitude IS NULL ORDER BY id"
    if args.limit:
        query += f" LIMIT {args.limit}"

    cur.execute(query)
    rows = cur.fetchall()
    total = len(rows)
    print(f"待 geocode 数量: {total}")

    success = 0
    failed = 0
    batch: list[tuple] = []

    for i, (listing_id, title, address) in enumerate(rows, 1):
        coords = geocode_one(title or "", address or "")

        if coords:
            lat, lng = coords
            batch.append((lat, lng, listing_id))
            success += 1
        else:
            failed += 1

        # 进度
        if i % 50 == 0 or i == total:
            pct = i / total * 100
            print(f"[{i}/{total}] {pct:.1f}% | 成功: {success} 失败: {failed}", flush=True)

        # 批量提交
        if len(batch) >= BATCH_SIZE:
            execute_batch(cur, "UPDATE listings SET latitude=%s, longitude=%s WHERE id=%s", batch)
            conn.commit()
            batch.clear()

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 提交剩余
    if batch:
        execute_batch(cur, "UPDATE listings SET latitude=%s, longitude=%s WHERE id=%s", batch)
        conn.commit()

    cur.close()
    conn.close()

    print(f"\n完成！成功: {success} / {total}，失败: {failed}")
    print(f"成功率: {success/total*100:.1f}%" if total > 0 else "无数据")


if __name__ == "__main__":
    main()
