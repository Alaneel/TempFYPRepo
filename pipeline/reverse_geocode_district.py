"""
Reverse Geocode District from Coordinates
==========================================
策略：
1. 取所有有坐标但无 district 的 listings
2. 用 OneMap reverseGeocode API (lat/lng → 最近地址 + 邮编)
3. 邮编前两位 → Singapore District (1-28)
4. UPDATE listings.district
5. 支持 --overwrite 覆盖已有 district（同伴数据来了用）

用法：
    python pipeline/reverse_geocode_district.py              # 只填 district IS NULL
    python pipeline/reverse_geocode_district.py --limit 500  # 测试500条
    python pipeline/reverse_geocode_district.py --overwrite  # 全部覆盖（同伴数据更新后用）
"""

import os
import sys
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import execute_batch
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

# 加载 .env（如果有）
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass  # python-dotenv 未安装时直接读系统环境变量

# ── DB 配置 ────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "real_estate_app")
DB_USER = os.getenv("DB_USER", "alanwang")
DB_PASS = os.getenv("DB_PASS", "")

# OneMap Reverse Geocode API
ONEMAP_REVERSE = "https://www.onemap.gov.sg/api/public/revgeocode"
# OneMap Forward Search API
ONEMAP_SEARCH = "https://www.onemap.gov.sg/api/common/elastic/search"

# 认证 token — 优先从环境变量/．env 读取，到期后运行 pipeline/refresh_onemap_token.py 自动刷新
ONEMAP_TOKEN = os.getenv("ONEMAP_TOKEN", "")

if not ONEMAP_TOKEN:
    print("❌ ONEMAP_TOKEN 未设置。请先运行: python pipeline/refresh_onemap_token.py")
    sys.exit(1)

# 每批提交 & 并发线程数（认证用户限速 3600次/分钟 = 60次/秒，30线程并发安全）
BATCH_SIZE = 200
MAX_WORKERS = 30

# ── 邮编前两位 → Singapore District ────────────────────────
POSTAL_PREFIX_TO_DISTRICT: dict[str, int] = {
    "01": 1, "02": 1, "03": 1, "04": 1, "05": 1, "06": 1,
    "07": 2, "08": 2,
    "14": 3, "15": 3, "16": 3,
    "09": 4, "10": 4,
    "11": 5, "12": 5, "13": 5,
    "17": 6,
    "18": 7, "19": 7,
    "20": 8, "21": 8,
    "22": 9, "23": 9,
    "24": 10, "25": 10, "26": 10, "27": 10,
    "28": 11, "29": 11, "30": 11,
    "31": 12, "32": 12, "33": 12,
    "34": 13, "35": 13, "36": 13, "37": 13,
    "38": 14, "39": 14, "40": 14, "41": 14,
    "42": 15, "43": 15, "44": 15, "45": 15,
    "46": 16, "47": 16, "48": 16,
    "49": 17, "50": 17, "81": 17,
    "51": 18, "52": 18,
    "53": 19, "54": 19, "55": 19, "82": 19,
    "56": 20, "57": 20,
    "58": 21, "59": 21,
    "60": 22, "61": 22, "62": 22, "63": 22, "64": 22,
    "65": 23, "66": 23, "67": 23, "68": 23,
    "69": 24, "70": 24, "71": 24,
    "72": 25, "73": 25,
    "77": 26, "78": 26,
    "75": 27, "76": 27,
    "79": 28, "80": 28,
}


def postal_to_district(postal: str) -> Optional[int]:
    """6位邮编 → district (1-28)，找不到返回 None"""
    if not postal or len(postal) < 2:
        return None
    prefix = postal[:2].zfill(2)
    return POSTAL_PREFIX_TO_DISTRICT.get(prefix)


def reverse_geocode(lat: float, lng: float) -> Optional[int]:
    """
    调用 OneMap reverseGeocode API，返回 district 或 None。
    API: GET /api/public/revgeocode?location=lat,lng&buffer=40&addressType=All&otherFeatures=N
    """
    headers = {"Authorization": ONEMAP_TOKEN}
    try:
        resp = requests.get(
            ONEMAP_REVERSE,
            params={
                "location": f"{lat},{lng}",
                "buffer": 40,
                "addressType": "All",
                "otherFeatures": "N",
            },
            headers=headers,
            timeout=8,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        results = data.get("GeocodeInfo", [])
        if not results:
            # buffer 扩大到 200m 重试
            resp2 = requests.get(
                ONEMAP_REVERSE,
                params={
                    "location": f"{lat},{lng}",
                    "buffer": 200,
                    "addressType": "All",
                    "otherFeatures": "N",
                },
                headers=headers,
                timeout=8,
            )
            if resp2.status_code != 200:
                return None
            results = resp2.json().get("GeocodeInfo", [])
            if not results:
                return None

        # 取第一个结果的邮编
        postal = str(results[0].get("POSTALCODE", "")).strip()
        return postal_to_district(postal)

    except Exception:
        return None


def geocode_by_address(address: str) -> Optional[int]:
    """
    用地址正向搜索 OneMap，返回 district 或 None。
    作为反向地理编码失败时的兜底。
    """
    if not address or not address.strip():
        return None
    headers = {"Authorization": ONEMAP_TOKEN}
    try:
        resp = requests.get(
            ONEMAP_SEARCH,
            params={
                "searchVal": address.strip(),
                "returnGeom": "N",
                "getAddrDetails": "Y",
                "pageNum": 1,
            },
            headers=headers,
            timeout=8,
        )
        if resp.status_code != 200:
            return None
        results = resp.json().get("results", [])
        if not results:
            return None
        postal = str(results[0].get("POSTAL", "")).strip()
        if postal in ("NIL", ""):
            return None
        return postal_to_district(postal)
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Reverse geocode district from lat/lng via OneMap")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少条（测试用）")
    parser.add_argument("--overwrite", action="store_true",
                        help="覆盖已有 district（默认只填 NULL，同伴数据来了用此选项）")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()

    # 查询目标：有坐标或有地址 + (district IS NULL 或 --overwrite)
    if args.overwrite:
        where = "(latitude IS NOT NULL AND longitude IS NOT NULL) OR (address IS NOT NULL AND address != '')"
        print("模式：--overwrite，将覆盖所有已有 district")
    else:
        where = "((latitude IS NOT NULL AND longitude IS NOT NULL) OR (address IS NOT NULL AND address != '')) AND district IS NULL"
        print("模式：只填 district IS NULL 的记录")

    query = f"SELECT id, latitude, longitude, address FROM listings WHERE {where} ORDER BY id"
    if args.limit:
        query += f" LIMIT {args.limit}"

    cur.execute(query)
    rows = cur.fetchall()
    total = len(rows)
    print(f"待处理数量: {total:,}")

    if total == 0:
        print("无需处理，退出。")
        cur.close()
        conn.close()
        return

    success_rev = 0   # 反向地理编码成功
    success_fwd = 0   # 地址正向搜索兜底成功
    failed = 0
    batch: list[tuple] = []

    # 统计各 district 分布（调试用）
    district_counts: dict[int, int] = {}
    t_start = time.time()

    def fetch_one(row):
        listing_id, lat, lng, address = row
        # 第一步：反向地理编码（坐标 → 邮编），坐标不存在则跳过
        if lat is not None and lng is not None:
            district = reverse_geocode(float(lat), float(lng))
            if district is not None:
                return listing_id, district, "rev"
        # 第二步：地址正向搜索兜底
        district = geocode_by_address(address)
        if district is not None:
            return listing_id, district, "fwd"
        return listing_id, None, "fail"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_one, row): row for row in rows}
        done = 0
        for future in as_completed(futures):
            done += 1
            listing_id, district, source = future.result()

            if district is not None:
                batch.append((district, listing_id))
                if source == "rev":
                    success_rev += 1
                else:
                    success_fwd += 1
                district_counts[district] = district_counts.get(district, 0) + 1
            else:
                failed += 1

            # 进度
            if done % 100 == 0 or done == total:
                elapsed = time.time() - t_start
                rps = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rps if rps > 0 else 0
                success = success_rev + success_fwd
                print(f"[{done:,}/{total:,}] {done/total*100:.1f}% | 成功: {success:,}(坐标:{success_rev} 地址:{success_fwd}) 失败: {failed:,} | {rps:.1f} req/s | ETA {eta:.0f}s", flush=True)

            # 批量提交
            if len(batch) >= BATCH_SIZE:
                execute_batch(cur, "UPDATE listings SET district=%s WHERE id=%s", batch)
                conn.commit()
                batch.clear()

    # 提交剩余
    if batch:
        execute_batch(cur, "UPDATE listings SET district=%s WHERE id=%s", batch)
        conn.commit()

    cur.close()
    conn.close()

    print(f"\n{'='*50}")
    elapsed_total = time.time() - t_start
    success = success_rev + success_fwd
    print(f"完成！成功: {success:,} / {total:,}，失败: {failed:,}，耗时: {elapsed_total:.1f}s ({total/elapsed_total:.1f} req/s)")
    print(f"  坐标反向: {success_rev:,}  地址正向: {success_fwd:,}")
    print(f"成功率: {success/total*100:.1f}%" if total > 0 else "")

    # 打印 district 分布
    if district_counts:
        print("\nDistrict 分布（Top 15）：")
        for d, cnt in sorted(district_counts.items(), key=lambda x: -x[1])[:15]:
            bar = "█" * (cnt * 30 // max(district_counts.values()))
            print(f"  D{d:2d}: {cnt:6,}  {bar}")


if __name__ == "__main__":
    main()
