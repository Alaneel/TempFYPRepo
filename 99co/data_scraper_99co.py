import asyncio
import random
import time
import os
from pathlib import Path
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup

# === CONFIG ==============================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
]

SEARCH_BASE = "https://www.99.co/singapore"

# Output path
# Output path
out_root = Path(__file__).resolve().parent.parent / "data" / "99co"
out_root.mkdir(parents=True, exist_ok=True)

# === CORE FUNCTIONS ======================================================

def parse_cards_to_df(html: str) -> pd.DataFrame:
    """Extract listings from one page of HTML."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div[data-testid='listing-card']")
    # Fallback to older selector if new one fails
    if not cards:
        cards = soup.select("[data-testid='grid-item-card-container']")

    data = []

    for c in cards:

        # --- Basic fields ---
        # Title can be in h2 or h3 depending on the card version, usually h3 with data-cy='listingName'
        title_el = c.select_one("h3[data-cy='listingName'], h2")
        title = title_el.get_text(strip=True) if title_el else ""

        # Price / PSF
        # Price often in li:nth-of-type(2) or a span with specific text. 
        # But looking at structure: usually first li is price, second is psf in the price-ul list
        price_el = c.select_one("[data-cy='listingPsfPrice'] li:nth-child(2)")
        psf_el = c.select_one("[data-cy='listingPsfPrice'] li:nth-child(3)")
        
        # Fallback if specific data-cy structure differs
        if not price_el:
             # Try finding element with $
             price_candidates = c.find_all(string=lambda s: s and "$" in s)
             for pc in price_candidates:
                 t = pc.strip()
                 if len(t) > 20: continue
                 # simple heuristic: if it has 'psf' it's psf, else price
                 if "psf" in t.lower():
                     if not psf_el: psf_tmp = t
                 else:
                     if not price_el: price_tmp = t

        price = price_el.get_text(strip=True) if price_el else ""
        psf = psf_el.get_text(strip=True) if psf_el else ""

        # --- Decompose Info ---
        beds = baths = sqft = tenure = built_year = ""
        
        # Select all list items in the info section
        info_items = c.select("ul li[class*='flex']") 
        # If the above is too specific/wrong, try the generic one and filter
        if not info_items:
             info_items = c.select("ul li")

        for item in info_items:
            txt = item.get_text(strip=True)
            prop = item.get("itemprop", "")
            
            if "bed" in txt.lower():
                beds = txt
            elif "bath" in txt.lower():
                baths = txt
            elif "sqft" in txt.lower():
                sqft = txt
            elif prop == "leaseLength" or "yr" in txt.lower() or "freehold" in txt.lower():
                tenure = txt
            elif prop == "yearbuilt" or "built" in txt.lower():
                built_year = txt

        # --- Address ---
        # Found selector: p.text-microcopy-12-regular.text-dark-neutral-100
        # Also typically ends with valid postal code (6 digits)
        addr_el = c.select_one("p.text-microcopy-12-regular.text-dark-neutral-100")
        if not addr_el:
             # Fallback: look for p tag with 6 digit number at end
             import re
             for p in c.select("p"):
                 if re.search(r"\d{6}$", p.get_text(strip=True)):
                     addr_el = p
                     break
        address = addr_el.get_text(strip=True) if addr_el else ""

        # --- Agent / Image / URL ---
        agent_el = c.select_one('[class*="text-foreground"]')
        agent = agent_el.get_text(strip=True) if agent_el else ""


        img_el = c.select_one("img[alt*='Project Photos']")
        image = img_el["src"] if img_el and img_el.has_attr("src") else ""

        data.append({
            "title": title,
            "display_price": price,
            "psf": psf,
            "beds": beds,
            "baths": baths,
            "sqft": sqft,
            "built_year": built_year,
            "tenure": tenure,
            "address": address,
            "agent_name": agent,
            "image_url": image,
        })

    return pd.DataFrame(data)


async def goto_with_retry(page, url: str, *, nav_timeout_ms: int, wait_selector: str | None,
                          wait_timeout_ms: int, retries: int, retry_forever: bool):
    """Navigate to a URL with retry logic and return (ok, status)."""
    attempt = 0
    while True:
        response_status = None
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            response_status = response.status if response else None
        except Exception as e:
            print(f"[nav warn] {e}. url={url}")

        try:
            # Scroll to trigger lazy load
            for _ in range(5):
                await page.mouse.wheel(0, 2000)
                await asyncio.sleep(0.6)
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=wait_timeout_ms)
            return True, response_status
        except PWTimeout:
            pass

        attempt += 1
        if not retry_forever and attempt > retries:
            print(f"[nav fail] giving up after {attempt-1} retries on {url}")
            return False, response_status

        backoff = min(2 ** min(attempt, 5), 30) + random.uniform(0, 0.5)
        print(f"[retry] attempt {attempt} in {backoff:.1f}s → {url}")
        await asyncio.sleep(backoff)

        try:
            if attempt % 3 == 0:
                await page.goto("about:blank", timeout=5000)
        except Exception:
            pass


def build_search_url(purpose="sale", page=1, page_size=36):
    return f"{SEARCH_BASE}/{purpose}?page_num={int(page)}&page_size={int(page_size)}&property_segments=residential"


async def scrape_one_type(p, browser, purpose, out_root, total_pages=9999,
                          nav_timeout_ms=30000, wait_timeout_ms=10000,
                          retries=3, retry_forever=False):
    """Scrape listings for one purpose (sale or rent)."""
    print(f"[info] {purpose.upper()} • starting")
    start_time = time.time()
    all_rows = []
    empty_streak = 0

    # Create first context
    user_agent = random.choice(USER_AGENTS)
    context = await browser.new_context(
        user_agent=user_agent,
        viewport={"width": 1366, "height": 768},
        locale="en-US",
    )
    page = await context.new_page()
    print(f"[session] started with UA: {user_agent}")

    for page_num in range(1, total_pages + 1):
        # Rotate context every 200 pages
        if page_num % 200 == 1 and page_num > 1:
            print(f"[rotate] restarting context at page {page_num}")
            try:
                await context.close()
            except Exception:
                pass
            user_agent = random.choice(USER_AGENTS)
            context = await browser.new_context(
                user_agent=user_agent,
                viewport={"width": 1366, "height": 768},
                locale="en-US",
            )
            page = await context.new_page()
            print(f"[rotate] new UA: {user_agent}")

        url = build_search_url(purpose, page_num)
        print(f"[info] {purpose.upper()} • Page {page_num}")

        ok, status = await goto_with_retry(
            page, url,
            nav_timeout_ms=nav_timeout_ms,
            wait_selector="[data-testid='grid-item-card-container']",
            wait_timeout_ms=wait_timeout_ms,
            retries=retries,
            retry_forever=retry_forever
        )

        if not ok or (status and status != 200):
            print(f"[stop] got HTTP {status} on page {page_num} → stopping crawl")
            break

        html = await page.content()
        df = parse_cards_to_df(html)
        if df.empty:
            empty_streak += 1
            print(f"[warn] page {page_num} had 0 listings ({empty_streak} empty in a row)")
            if empty_streak >= 2:
                print("[stop] 2 consecutive empty pages → stopping crawl")
                break
        else:
            empty_streak = 0
            
            df["purpose"] = purpose
            
            # Simple inference for prop_type from title
            def infer_type(t):
                t = str(t).upper()
                if "HDB" in t: return "HDB"
                if "CONDO" in t or "APARTMENT" in t: return "Condo"
                if "LANDED" in t or "DETACHED" in t or "TERRACE" in t: return "Landed"
                return "Residential"
            
            df["prop_type"] = df["title"].apply(infer_type)
            
            all_rows.append(df)
            print(f"[ok] {len(df)} listings")

        await asyncio.sleep(random.uniform(2.0, 5.0))

    await context.close()
    elapsed = time.time() - start_time

    if all_rows:
        final_df = pd.concat(all_rows, ignore_index=True)
        csv_path = out_root / f"{purpose}_listings.csv"
        final_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[ok] wrote {len(final_df)} rows → {csv_path}")
    else:
        print(f"[info] no listing rows scraped in total.")

    print(f"[time] {purpose.upper()} took {elapsed:.2f}s")

# === MAIN ================================================================

async def run_all(purpose: str, max_pages: int, headless: bool):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )

        await scrape_one_type(p, browser, purpose, out_root, total_pages=max_pages)
        await browser.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--purpose", choices=["sale", "rent", "both"], required=True)
    ap.add_argument("--max-pages", type=int, default=9999)
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    start_time = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[start] {start_time} • purposes={[args.purpose]}")

    purposes = ["sale", "rent"] if args.purpose == "both" else [args.purpose]
    for purpose in purposes:
        asyncio.run(run_all(purpose, args.max_pages, args.headless))

    print("[done] all complete.")
