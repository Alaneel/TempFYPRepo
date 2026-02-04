# data_scraper_6.py
# SRX scraper with per-town fail-safe CSVs:
# - Immediately writes a CSV right after each town finishes (with time-taken in filename)
# - SALE/Rent/both (SALE first when both)
# - Parallel towns, retries, optional media blocking
# - Broader selectors + cookie accept + lazy-load scroll
# - Writes one combined CSV + timings Excel at the end

import argparse, asyncio, re, time, random
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import (
    async_playwright,
    TimeoutError as PWTimeout,
    Page,
    BrowserContext,
    Route,
    Request,
)

BASE = "https://www.srx.com.sg"
SEARCH_BASE = f"{BASE}/search"

# Forgiving selector set (site sometimes varies classes)
CARD_SEL = ".listing.listingView, .grid-item.listing, div.listing.listingView, div[class*='listingView']"

# ---------------- helpers ----------------

def fmt_dur_hms_tag(seconds: float) -> str:
    """Return 01h02m03s tag for filenames."""
    s = int(max(0, seconds))
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}h{m:02d}m{s:02d}s"

def fmt_dur_sec(s: float) -> str:
    return str(timedelta(seconds=int(max(0, s))))

def build_search_url(purpose="rent", town_id=26, page=None, view="list"):
    qs = f"residential?selectedHdbTownIds={int(town_id)}&view={view}"
    if page and page > 1:
        qs += f"&page={int(page)}"
    return f"{SEARCH_BASE}/{purpose}/{qs}"

def find_pages_in_html(html: str) -> list[int]:
    soup = BeautifulSoup(html, "lxml")
    pages = {1}
    for a in soup.select("a[href*='?']"):
        href = a.get("href") or ""
        try:
            full = urljoin(BASE, href)
            qs = parse_qs(urlparse(full).query)
            if "page" in qs:
                p = int(qs["page"][0])
                if 1 <= p <= 9999:
                    pages.add(p)
        except:
            pass
    for el in soup.select("[data-page]"):
        try:
            p = int(el["data-page"])
            if 1 <= p <= 9999:
                pages.add(p)
        except:
            pass
    return sorted(pages)

def _text(el): 
    return el.get_text(strip=True) if el else ""

def _attr(el, name, default=""):
    if not el: return default
    val = el.get(name, default)
    if name == "class" and isinstance(val, (list, tuple)): 
        return " ".join(val)
    return val if val is not None else default

def _norm_str(x): 
    return "" if x in (None, "null", "None", "NULL") else str(x)

def dump_debug_html(html: str, purpose: str, town_id: int, page_no: int, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    fn = out_dir / f"DEBUG_{purpose}_town{town_id}_p{page_no}.html"
    fn.write_text(html, encoding="utf-8")
    print(f"[debug] saved {fn}")

# ---------------- parser ----------------

def parse_cards_to_df(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(CARD_SEL)
    rows = []

    def _bg_url(s: str) -> str:
        if isinstance(s, str) and "background-image" in s:
            m = re.search(r"url\(['\"]?(.*?)['\"]?\)", s)
            return m.group(1) if m else ""
        return s or ""

    for c in cards:
        is_sale = _attr(c, "issale")

        hidden_inputs = {}
        hb = c.select_one(".agentMobileNoDiv")
        if hb:
            for inp in hb.select("input"):
                cls_str = _attr(inp, "class"); typ_str = _attr(inp, "type")
                hidden_inputs[f"{cls_str}:{typ_str}"] = _attr(inp, "value")
                key = _attr(inp, "name") or (cls_str.split()[-1] if cls_str else "") or _attr(inp, "id") or typ_str
                if key: hidden_inputs[key] = _attr(inp, "value")

        listing_id = _norm_str(hidden_inputs.get("listing-id"))

        title_a = c.select_one("a.listingDetailTitle")
        title = _text(title_a.select_one("span.notranslate") if title_a else None) or _text(title_a)
        href = urljoin(BASE, _attr(title_a, "href")) if title_a else ""

        price = _text(c.select_one(".listingDetailPrice")) or _text(c.select_one(".listingDetailPriceGrid")) or _norm_str(hidden_inputs.get("listing-price"))

        prop_type = tenure = ""
        tb = c.select_one(".listingDetailType")
        if tb:
            spans = [s.get_text(strip=True) for s in tb.select("span")]
            if spans: prop_type = spans[0]
            m = re.search(r"(freehold|leasehold[-\s]?\d+|99[-\s]?year|999[-\s]?year)", tb.get_text(" ", strip=True), re.I)
            if m: tenure = m.group(1)

        size_sqft = psf = ""
        sl = _text(c.select_one(".listingDetailValues"))
        if sl:
            m1 = re.search(r"([\d,]+)\s*sqft", sl, re.I);  
            m2 = re.search(r"\$[\d,]+\s*psf", sl, re.I)
            if m1: size_sqft = m1.group(1)
            if m2: psf = m2.group(0)
        build_psf = _norm_str(hidden_inputs.get("build-psf"))
        if not psf and build_psf:
            try: psf = f"${float(build_psf):,.0f} psf"
            except: psf = build_psf

        beds = _text(c.select_one(".listingDetailRoomNo"))
        baths = _text(c.select_one(".listingDetailToiletNo"))

        photo_a = c.select_one("a.listingPhoto")
        photo = _attr(photo_a, "listing-photo") or _bg_url(_attr(photo_a, "style"))

        posted_age = _text(c.select_one(".listing-date-posted span"))
        posted_epoch = _attr(c.select_one(".listing-date-posted"), "data-date")

        addr = _text(c.select_one(".listingDetailAgentAgencyText"))

        agent_name_a = c.select_one(".listingDetailAgentName")
        agent_name = _text(agent_name_a)
        agent_profile = urljoin(BASE, _attr(agent_name_a, "href")) if agent_name_a else ""

        agent_photo_a = c.select_one(".listingAgentPhoto")
        agent_photo = _bg_url(_attr(agent_photo_a, "style"))

        agent_phone_masked = _text(c.select_one(".agentMobileNo"))
        agent_phone_full = _norm_str(hidden_inputs.get("mobile-number-full"))
        agent_user_id = _norm_str(hidden_inputs.get("agent-user-id"))
        agency_id = _norm_str(hidden_inputs.get("agency-id"))

        call_a = c.select_one(".button.callButton"); whatsapp_a = c.select_one(".button.whatsappButton")
        agent_call = _attr(call_a, "href"); agent_whatsapp = _attr(whatsapp_a, "href")

        project_name  = _norm_str(hidden_inputs.get("project-name"))
        town          = _norm_str(hidden_inputs.get("district-town"))
        postal        = _norm_str(hidden_inputs.get("postal"))
        built_size    = _norm_str(hidden_inputs.get("built-size"))
        created_date  = _norm_str(hidden_inputs.get("created-date"))
        expiry_date   = _norm_str(hidden_inputs.get("expiry-date"))
        latlong       = _norm_str(hidden_inputs.get("lat-long"))
        
        # Enhanced from skwips: extract built_year and full_address
        built_year_raw = _norm_str(hidden_inputs.get("built-year"))
        built_year = ""
        if built_year_raw:
            # Remove "Built-" prefix if present
            built_year_clean = re.sub(r'Built-?', '', built_year_raw, flags=re.I).strip()
            if built_year_clean.isdigit():
                built_year = built_year_clean
        
        full_address = _norm_str(hidden_inputs.get("project-full-address"))
        if not full_address and project_name:
            # Fallback: combine project name with postal
            full_address = f"{project_name} {postal}".strip() if postal else project_name

        rows.append({
            "listing_id": listing_id, "title": title, "url": href, "price": price,
            "prop_type": prop_type, "tenure": tenure, "size_sqft": size_sqft, "psf": psf,
            "beds": beds, "baths": baths, "address": addr, "photo": photo,
            "posted_age": posted_age, "posted_epoch": posted_epoch, "is_sale": is_sale,
            "project_name": project_name, "town": town, "postal": postal,
            "built_size_hidden": built_size, "created_date": created_date, "expiry_date": expiry_date,
            "lat_long": latlong, "agent_name": agent_name, "agent_profile": agent_profile,
            "agent_photo": agent_photo, "agent_phone_masked": agent_phone_masked,
            "agent_phone_full": agent_phone_full, "agent_user_id": agent_user_id, "agency_id": agency_id,
            "agent_call": agent_call, "agent_whatsapp": agent_whatsapp,
            "built_year": built_year,  # NEW from skwips
            "full_address": full_address,  # NEW from skwips
        })
    return pd.DataFrame(rows)

# ---------------- resilient navigation ----------------

async def goto_with_retry(page: Page, url: str, *, nav_timeout_ms: int, wait_selector: str|None, wait_timeout_ms: int, retries: int, retry_forever: bool) -> bool:
    attempt = 0
    while True:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
        except Exception as e:
            print(f"[nav warn] {e}. url={url}")
        try:
            # nudge lazy-loaders
            await page.mouse.wheel(0, 1200)
            await asyncio.sleep(0.2)
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=wait_timeout_ms)
            return True
        except PWTimeout:
            pass
        attempt += 1
        if not retry_forever and attempt > retries:
            print(f"[nav fail] giving up after {attempt-1} retries on {url}")
            return False
        backoff = min(2 ** min(attempt, 5), 30) + random.uniform(0, 0.5)
        print(f"[retry] attempt {attempt} in {backoff:.1f}s → {url}")
        await asyncio.sleep(backoff)
        try:
            if attempt % 3 == 0:
                await page.goto("about:blank", timeout=5000)
        except Exception:
            pass

# ---------------- one town (writes per-town CSV here) ----------------

async def scrape_one_town(ctx: BrowserContext, town_id: int, purpose: str, *, out_root: Path,
                          nav_timeout_ms: int, wait_timeout_ms: int, max_pages: int|None,
                          retries: int, retry_forever: bool):
    """
    Returns dict: {df, elapsed, rows, town_id, purpose, page_times:[{page,elapsed,rows}...] }
    Also writes a fail-safe per-town CSV immediately when data is available.
    """
    town_start = time.perf_counter()
    page = await ctx.new_page()

    first_url = build_search_url(purpose, town_id, page=None, view="list")
    print(f"[info] {purpose.upper()} • Town {town_id} • Open: {first_url}")

    ok = await goto_with_retry(page, first_url, nav_timeout_ms=nav_timeout_ms, wait_selector=CARD_SEL, wait_timeout_ms=wait_timeout_ms, retries=retries, retry_forever=retry_forever)
    if not ok:
        await page.close()
        return {"df": None, "elapsed": 0.0, "rows": 0, "town_id": town_id, "purpose": purpose, "page_times": []}

    # try accept cookie
    for sel in ["button:has-text('Accept')", "text=Accept", "button:has-text('I Agree')"]:
        try:
            if await page.locator(sel).count() > 0:
                await page.locator(sel).first.click()
                await asyncio.sleep(0.3)
        except:
            pass

    # initial scroll pulses
    for _ in range(2):
        await page.mouse.wheel(0, 2500)
        await asyncio.sleep(0.25)

    html = await page.content()
    total_pages = max(find_pages_in_html(html) or [1])
    if max_pages:
        total_pages = min(total_pages, max_pages)
    total_pages = min(total_pages, 100)

    all_frames = []
    page_times = []
    empty_streak = 0
    for p in range(1, total_pages + 1):
        pstart = time.perf_counter()
        url = build_search_url(purpose, town_id, page=p, view="list")
        print(f"[info] {purpose.upper()} • Town {town_id} • Page {p}/{total_pages}")
        ok = await goto_with_retry(page, url, nav_timeout_ms=nav_timeout_ms, wait_selector=CARD_SEL, wait_timeout_ms=wait_timeout_ms, retries=retries, retry_forever=retry_forever)
        if not ok:
            print(f"[warn] town {town_id} page {p} failed after retries; continuing")
            page_times.append({"purpose": purpose, "town_id": town_id, "page": p, "elapsed": None, "rows": 0})
            continue

        # lazy-load pulses on each page
        for _ in range(2):
            await page.mouse.wheel(0, 2000)
            await asyncio.sleep(0.2)

        html = await page.content()
        if p == 1:
            dump_debug_html(html, purpose, town_id, p, Path("debug_pages"))

        df = parse_cards_to_df(html)
        dur = time.perf_counter() - pstart
        page_times.append({"purpose": purpose, "town_id": town_id, "page": p, "elapsed": dur, "rows": len(df)})
        print(f"[time] town {town_id} page {p} took {fmt_dur_sec(dur)} — rows {len(df)}")

        if df.empty:
            dump_debug_html(html, purpose, town_id, p, Path("debug_pages"))
            empty_streak += 1
            if empty_streak >= 2 and not retry_forever:
                print(f"[info] town {town_id} hit 2 empty pages → stop")
                break
            continue

        empty_streak = 0
        df["town_id"] = town_id
        df["purpose"] = purpose
        df["page"] = p
        all_frames.append(df)

    await page.close()
    elapsed = time.perf_counter() - town_start
    out_df = pd.concat(all_frames, ignore_index=True).drop_duplicates() if all_frames else None
    rows = 0 if out_df is None else len(out_df)
    print(f"[time] {purpose.upper()} • Town {town_id} took {fmt_dur_sec(elapsed)} — rows {rows}")

    # ===== FAIL-SAFE: write per-town CSV immediately if any rows =====
    if out_df is not None and not out_df.empty:
        tag = fmt_dur_hms_tag(elapsed)
        fn = out_root / f"{purpose}_town_{town_id}_{tag}_{rows}rows.csv"
        out_df.to_csv(fn, index=False, encoding="utf-8-sig")
        print(f"[ok] Wrote per-town CSV -> {fn}")

    return {"df": out_df, "elapsed": elapsed, "rows": rows, "town_id": town_id, "purpose": purpose, "page_times": page_times}

# ---------------- runner ----------------

def parse_town_ids(s: str) -> list[int]:
    ids = set()
    for part in s.split(","):
        part = part.strip()
        if not part: 
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            ids.update(range(int(a), int(b) + 1))
        else:
            ids.add(int(part))
    return sorted(ids)

async def run_all(purpose: str, out_dir: str|None, towns: list[int], concurrency: int, nav_timeout_ms: int, wait_timeout_ms: int, max_pages: int|None, block_media: bool, headless: bool, retries: int, retry_forever: bool):
    out_root = Path(out_dir) if out_dir else (Path(__file__).resolve().parent.parent / "data" / "srx")
    out_root.mkdir(parents=True, exist_ok=True)

    start_local = datetime.now()
    total_start = time.perf_counter()
    purposes = ["sale", "rent"] if purpose == "both" else [purpose]
    print(f"[start] {start_local:%Y-%m-%d %H:%M:%S} • purposes={purposes} • towns={towns} • conc={concurrency}")

    async with async_playwright() as play:
        browser = await play.chromium.launch(headless=headless)
        ctx = await browser.new_context(viewport={"width": 1366, "height": 900})

        if block_media:
            async def handler(route: Route, request: Request):
                rt = request.resource_type; url = request.url
                if rt in {"image", "media", "font", "stylesheet"}: 
                    return await route.abort()
                if any(s in url for s in ["google-analytics","gtm","googletagmanager","doubleclick","hotjar","facebook","mixpanel"]): 
                    return await route.abort()
                await route.continue_()
            await ctx.route("**/*", handler)

        all_results = []
        all_dfs = []
        for purp in purposes:
            sem = asyncio.Semaphore(max(1, concurrency))
            tasks = []
            for tid in towns:
                async def run_one(town_id=tid, pur=purp):
                    async with sem:
                        return await scrape_one_town(
                            ctx, town_id, pur, out_root=out_root,
                            nav_timeout_ms=nav_timeout_ms, wait_timeout_ms=wait_timeout_ms,
                            max_pages=max_pages, retries=retries, retry_forever=retry_forever
                        )
                tasks.append(run_one())
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    print(f"[warn] task failed: {r}")
                    continue
                all_results.append(r)
                if isinstance(r.get("df"), pd.DataFrame) and not r["df"].empty:
                    all_dfs.append(r["df"])

        await ctx.close()
        await browser.close()

    # combine listings
    csv_path = None
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
        csv_name = "rent_sale_all_towns.csv" if purpose == "both" else f"{purposes[0]}_all_towns.csv"
        csv_path = out_root / csv_name
        combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[ok] Saved combined {len(combined)} rows -> {csv_path}")
    else:
        print("[info] No listing rows scraped in total (but check per-town CSVs).")

    # build timings Excel (guard for empty)
    town_rows = []
    page_rows = []
    for r in all_results:
        town_rows.append({"town_id": r["town_id"], "purpose": r["purpose"], "elapsed_sec": r["elapsed"], "rows": r["rows"]})
        for pr in r["page_times"]:
            page_rows.append({
                "purpose": pr["purpose"], "town_id": pr["town_id"], "page": pr["page"],
                "elapsed_sec": pr["elapsed"] if pr["elapsed"] is not None else "",
                "rows": pr["rows"]
            })

    df_town = pd.DataFrame(town_rows)
    # pages sheet
    if page_rows:
        df_pages = pd.DataFrame(page_rows).sort_values(["purpose","town_id","page"])
    else:
        df_pages = pd.DataFrame(columns=["purpose","town_id","page","elapsed_sec","rows"])

    def hms_col(s):
        return s.apply(lambda x: fmt_dur_sec(x) if pd.notnull(x) and x != "" else "")

    if not df_town.empty:
        towns_sorted = sorted(set(towns))
        pivot_sec = df_town.pivot_table(index="town_id", columns="purpose", values="elapsed_sec", aggfunc="sum").reindex(towns_sorted)
        pivot_rows = df_town.pivot_table(index="town_id", columns="purpose", values="rows", aggfunc="sum").reindex(towns_sorted)
        for col in ["sale", "rent"]:
            if col not in pivot_sec.columns: pivot_sec[col] = pd.NA
            if col not in pivot_rows.columns: pivot_rows[col] = pd.NA

        report = pd.DataFrame({
            "Sale_sec": pivot_sec["sale"],
            "Sale_hms": hms_col(pivot_sec["sale"]),
            "Sale_rows": pivot_rows["sale"],
            "Rent_sec": pivot_sec["rent"],
            "Rent_hms": hms_col(pivot_sec["rent"]),
            "Rent_rows": pivot_rows["rent"],
        })
        report.index.name = "town_id"
    else:
        report = pd.DataFrame(columns=["Sale_sec","Sale_hms","Sale_rows","Rent_sec","Rent_hms","Rent_rows"])

    meta = pd.DataFrame([{
        "started_at": start_local.strftime("%Y-%m-%d %H:%M:%S"),
        "total_time_hms": fmt_dur_sec(time.perf_counter() - total_start),
        "purposes": ",".join(purposes),
        "num_towns": len(towns),
        "concurrency": concurrency,
        "nav_timeout_ms": nav_timeout_ms,
        "wait_timeout_ms": wait_timeout_ms,
        "max_pages": max_pages if max_pages else "",
        "block_media": block_media,
        "retries": retries,
        "retry_forever": retry_forever,
        "combined_csv": str(csv_path) if csv_path else "",
    }])

    # Excel or CSV fallback
    xlsx_name = "timings_rent_sale.xlsx" if purpose == "both" else f"timings_{purposes[0]}.xlsx"
    xlsx_path = out_root / xlsx_name
    try:
        import openpyxl  # ensure available
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            report.to_excel(writer, sheet_name="Town_Timings")
            df_pages.to_excel(writer, sheet_name="Page_Timings", index=False)
            meta.to_excel(writer, sheet_name="Run_Meta", index=False)
        print(f"[ok] Wrote timings Excel -> {xlsx_path}")
    except ModuleNotFoundError:
        # CSV fallbacks if openpyxl is missing
        report.to_csv(out_root / "timings_town.csv", index=True, encoding="utf-8-sig")
        df_pages.to_csv(out_root / "timings_pages.csv", index=False, encoding="utf-8-sig")
        meta.to_csv(out_root / "timings_meta.csv", index=False, encoding="utf-8-sig")
        print("[warn] openpyxl not installed; wrote CSV timing files instead.")

    print(f"[total time] {fmt_dur_sec(time.perf_counter() - total_start)}")
    return str(xlsx_path)

def cli():
    ap = argparse.ArgumentParser(description="SRX scraper with per-town fail-safe CSVs")
    ap.add_argument("--purpose", choices=["rent", "sale", "both"], default="rent", help="SALE runs first when both")
    ap.add_argument("--town", type=int, help="Single town ID")
    ap.add_argument("--towns", type=str, default="1-28", help='Comma/range list, e.g. "1-28" or "1,3,5-7"')
    ap.add_argument("--out", type=str, default="", help='Output folder, e.g. "C:\\Users\\D\\Desktop\\ABC"')
    ap.add_argument("--concurrency", type=int, default=6)
    ap.add_argument("--nav-timeout-ms", type=int, default=30000)
    ap.add_argument("--wait-timeout-ms", type=int, default=6000)
    ap.add_argument("--max-pages", type=int, default=None)
    ap.add_argument("--block-media", action="store_true")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--retries", type=int, default=5)
    ap.add_argument("--retry-forever", action="store_true")
    return ap.parse_args()

def parse_town_ids_cli(val: str) -> list[int]:
    if not val:
        return list(range(1, 29))
    if val == "1-28":
        return list(range(1, 29))
    return parse_town_ids(val)

if __name__ == "__main__":
    args = cli()
    towns = [args.town] if args.town else parse_town_ids_cli(args.towns)
    out_dir = args.out if args.out else None
    asyncio.run(
        run_all(
            purpose=args.purpose,
            out_dir=out_dir,
            towns=towns,
            concurrency=max(1, args.concurrency),
            nav_timeout_ms=max(1000, args.nav_timeout_ms),
            wait_timeout_ms=max(1000, args.wait_timeout_ms),
            max_pages=args.max_pages,
            block_media=args.block_media,
            headless=args.headless,
            retries=max(0, args.retries),
            retry_forever=args.retry_forever,
        )
    )
