import asyncio
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import random
import json
import re

# ---------------------------------------------------------------------------
# Configurable paths
# Configurable paths
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "edgeprop"
OUT_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR = OUT_DIR / "debug_pages"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Build URL from type + purpose
def build_url(purpose: str, prop_type: str, page: int) -> str:
    """Return the correct EdgeProp search URL for each property category."""
    listing_type = "rental" if purpose == "rental" else "sale"

    # full property_type sets for each category
    prop_type_map = {
        "hdb": "13%2C75%2C76%2C77%2C78%2C79%2C80%2C81%2C82%2C83%2C84%2C85%2C86%2C87%2C88%2C89%2C90%2C91%2C92%2C93%2C94%2C95%2C96%2C97%2C98%2C99%2C100%2C101%2C102%2C134%2C135%2C136",
        "condo": "9%2C103%2C104%2C105%2C106%2C107",
        "landed": "21%2C108%2C109%2C110%2C111%2C112%2C113%2C114%2C115%2C116%2C117%2C118",
    }
    code = {"hdb": "h", "condo": "r", "landed": "l"}[prop_type.lower()]
    property_type = prop_type_map[prop_type.lower()]

    return (
        f"https://www.edgeprop.sg/property-search?"
        f"listing_type={listing_type}"
        f"&property_type={property_type}"
        f"&page={page}&pageSize=20"
        f"&order_by=recommended&is_search=true"
        f"&property_type_code={code}"
        f"&with_new_launches=1&exclude_test_listing=true"
    )

# ---------------------------------------------------------------------------
# JSON parsing from __NEXT_DATA__ (merged from skwips - more reliable method)
def extract_mobile_from_url(url: str) -> str:
    """Extract mobile number from agent URL (e.g., .../Alan-Tang-90672388)"""
    if not url:
        return ""
    match = re.search(r'(\d{8,})', url)
    return match.group(1) if match else ""

def parse_json_listings(json_data: dict) -> pd.DataFrame:
    """Parse listings from EdgeProp's __NEXT_DATA__ JSON structure.
    
    This is more reliable than HTML parsing and provides access to more fields
    including agent CEA, mobile, and agency information.
    """
    try:
        listings = json_data.get('props', {}).get('pageProps', {}).get('listings', [])
    except (KeyError, TypeError):
        return pd.DataFrame()
    
    if not listings:
        return pd.DataFrame()
    
    data = []
    for listing in listings:
        # Build URL
        listing_url = listing.get('url', '')
        if listing_url and not listing_url.startswith('http'):
            listing_url = f"https://www.edgeprop.sg/{listing_url}"
        
        # Property info
        title = listing.get('project_name') or listing.get('title', '')
        
        # Price
        asking_price = listing.get('asking_price')
        price = ""
        if asking_price:
            try:
                price = f"S$ {int(float(asking_price)):,}"
            except:
                price = str(asking_price)
        
        # PSF
        psf = listing.get('asking_unit_price_psf', '')
        if psf:
            try:
                psf = f"S$ {float(psf):,.2f} psf"
            except:
                pass
        
        # Property type
        prop_type = listing.get('property_type') or listing.get('property_sub_type') or ''
        
        # Address
        street = listing.get('street_name', '')
        postal = listing.get('postal_code', '')
        address = f"{street} {postal}".strip()
        
        # District info
        district = listing.get('district_name') or listing.get('asset_district', '')
        region = listing.get('planning_region', '')
        nearby_parts = []
        if district:
            nearby_parts.append(f"District {district}")
        if region:
            nearby_parts.append(region)
        nearby_text = ', '.join(nearby_parts)
        
        # Agent details (enhanced from skwips)
        agent_name = listing.get('agent_name', '')
        agent_url = listing.get('agent_url', '')
        agent_photo = listing.get('agent_photo', '')
        agent_cea = listing.get('agent_id', '')  # CEA registration number
        agent_mobile = extract_mobile_from_url(agent_url)
        agency = listing.get('agency_name', '')
        
        # Other details
        bedrooms = listing.get('bedrooms', '')
        bathrooms = listing.get('bathrooms', '')
        floor_area = listing.get('floor_area_sqft', '')
        year_completed = listing.get('year_completed', '')
        tenure = listing.get('tenure', '')
        
        # Recency
        updated_at = listing.get('updatedAt', '')
        recency_text = f"Updated: {updated_at[:10]}" if updated_at else ''
        
        data.append({
            "title": title,
            "price": price,
            "psf": psf,
            "beds": str(bedrooms) if bedrooms else '',
            "baths": str(bathrooms) if bathrooms else '',
            "size_sqft": str(floor_area) if floor_area else '',
            "built_year": str(year_completed) if year_completed else '',
            "tenure": tenure,
            "address": address,
            "district": nearby_text,
            "agent": agent_name,
            "agent_url": agent_url,
            "agent_photo": agent_photo,
            "agent_cea": agent_cea,  # NEW
            "agent_mobile": agent_mobile,  # NEW
            "agency": agency,  # NEW
            "url": listing_url,
            "listing_id": listing_url.split("?")[0].split("/")[-1] if listing_url else "",
            "recency_text": recency_text,  # NEW
        })
    
    return pd.DataFrame(data)


def parse_cards_to_df(html: str, debug: bool = False) -> pd.DataFrame:
    """Parse one page of listings HTML into a dataframe."""
    soup = BeautifulSoup(html, "html.parser")
    listing_links = soup.select("a[href*='listing/']")
    print(f"[debug] Found {len(listing_links)} listing links")

    if not listing_links:
        print("[warn] No listing links found!")
        return pd.DataFrame()

    data, seen_urls = [], set()

    for idx, link in enumerate(listing_links):
        try:
            href = link.get("href", "")
            if not href:
                continue
            if href.startswith("http"):
                url = href
            else:
                # Handle relative paths that might not start with /
                if not href.startswith("/"):
                    href = "/" + href
                url = f"https://www.edgeprop.sg{href}"
            if url in seen_urls:
                continue
            seen_urls.add(url)

            card = link
            for _ in range(6):
                if card and card.name == "div" and len(card.get_text(strip=True)) > 50:
                    break
                card = card.parent

            if not card:
                continue

            # --- extract fields ---
            title_el = card.select_one("h2, h3, [class*='title'] h2, [class*='title']")
            title = title_el.get_text(strip=True) if title_el else ""

            price = ""
            price_el = card.select_one("[class*='price']")
            if price_el:
                price = price_el.get_text(strip=True)
            else:
                for elem in card.find_all(string=lambda s: s and "$" in s):
                    txt = str(elem).strip()
                    if txt and len(txt) < 40:
                        price = txt
                        break

            # --- improved bed/bath extraction ---
            beds = baths = ""
            info_items = card.select(".search-listing-card-info-item, span[class*='info-item']")
            if info_items:
                for item in info_items:
                    txt = item.get_text(strip=True)
                    if "bed" in txt.lower():
                        beds = txt
                    elif "bath" in txt.lower():
                        baths = txt
            else:
                 # Fallback
                 bedbath_container = card.find(string=lambda s: s and ("bed" in s.lower() or "bath" in s.lower()))
                 if bedbath_container:
                    text = bedbath_container.get_text(strip=True) if hasattr(bedbath_container, "get_text") else str(bedbath_container)
                    parts = [p.strip() for p in text.replace("beds", "bed").replace("baths", "bath").split("|")]
                    for p in parts:
                        if "bed" in p: beds = p
                        elif "bath" in p: baths = p

            agent = ""
            agent_el = card.select_one("[class*='agent'] [class*='name'], .search-listing-card-agent-name")
            if agent_el:
                agent = agent_el.get_text(strip=True)

            image = ""
            img_el = card.select_one("img")
            if img_el:
                image = img_el.get("src", "") or img_el.get("data-src", "")

            # --- Detail Parsing ---
            psf = ""
            district = ""
            built_year = ""
            size_sqft = ""
            tenure = ""
            address = ""

            detail_items = card.select(".search-listing-card-detail-item, span[class*='detail-item']")
            desc_list = [d.get_text(strip=True) for d in detail_items]
            
            # If explicit classes failed, try the old generic desc selector as fallback
            if not desc_list:
                for desc in card.select("[class*='desc']"):
                    txt = desc.get_text(strip=True)
                    if txt and txt not in desc_list:
                        desc_list.append(txt)

            for txt in desc_list:
                if "PSF" in txt.upper():
                    psf = txt
                elif re.match(r"^D\d+", txt):
                    district = txt
                elif "Built:" in txt:
                    built_year = txt.replace("Built:", "").strip()
                elif "sqft" in txt.lower():
                    size_sqft = txt
                elif any(t in txt.lower() for t in ["freehold", "99 year", "leasehold", "999 year"]):
                    tenure = txt
            
            # Address is typically the last item if it's not one of the others
            if desc_list:
                potential_addr = desc_list[-1]
                if potential_addr not in [psf, district, built_year, size_sqft, tenure]:
                    address = potential_addr

            details = " | ".join(desc_list)
            all_text = card.get_text(" | ", strip=True)

            data.append({
                "title": title,
                "price": price,
                "beds": beds,
                "baths": baths,
                "district": district,
                "psf": psf,
                "built_year": built_year,
                "size_sqft": size_sqft,
                "tenure": tenure,
                "address": address,
                "agent": agent,
                "url": url,
                "listing_id": url.split("?")[0].split("/")[-1] if url else "",
                "image": image,
                "details": details, # Keeping original details column too
                "raw_text": all_text[:300],
            })

        except Exception as e:
            print(f"[warn] Error parsing listing {idx}: {e}")
            continue

    print(f"[debug] Parsed {len(data)} unique listings")
    return pd.DataFrame(data)

# ---------------------------------------------------------------------------
async def scrape_edgeprop(purpose: str, prop_type: str, max_pages: int, headless: bool):
    """Main asynchronous scraper loop."""
    print(f"[start] {datetime.now():%Y-%m-%d %H:%M:%S} • purpose={purpose}, type={prop_type}")

    out_csv = OUT_DIR / f"edgeprop_{purpose}_{prop_type}.csv"
    all_rows, timings = [], []
    stop_flag = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        for i in range(1, max_pages + 1):
            url = build_url(purpose, prop_type, i)
            print(f"\n[info] {purpose.upper()} {prop_type.upper()} • Page {i}")
            print(f"[url] {url}")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(3)

                for sel in ["a[href*='listing/']", "div[class*='gallery']", "div[class*='card']"]:
                    try:
                        await page.wait_for_selector(sel, timeout=5000)
                        print(f"[ok] Found selector: {sel}")
                        break
                    except:
                        continue

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)

                html = await page.content()
                debug_path = DEBUG_DIR / f"DEBUG_{purpose}_{prop_type}_p{i}.html"
                debug_path.write_text(html, encoding="utf-8")
                await page.screenshot(path=str(DEBUG_DIR / f"DEBUG_{purpose}_{prop_type}_p{i}.png"), full_page=True)

                # Try JSON parsing first (from skwips - more reliable)
                df = pd.DataFrame()
                try:
                    next_data = await page.evaluate(
                        "() => document.getElementById('__NEXT_DATA__')?.textContent"
                    )
                    if next_data:
                        json_data = json.loads(next_data)
                        df = parse_json_listings(json_data)
                        if not df.empty:
                            print(f"[ok] Parsed {len(df)} listings from JSON")
                except Exception as e:
                    print(f"[warn] JSON parsing failed: {e}, falling back to HTML")

                # Fallback to HTML parsing
                if df.empty:
                    df = parse_cards_to_df(html, debug=(i == 1))
                
                if df.empty:
                    print(f"[stop] no listings found on page {i} → stopping crawl")
                    stop_flag = True
                    break

                df["purpose"] = purpose
                df["prop_type"] = prop_type

                all_rows.append(df)
                timings.append({"page": i, "rows": len(df), "time": datetime.now()})
                print(f"[ok] page {i} scraped ({len(df)} rows)")

            except Exception as e:
                print(f"[fail] Page {i} error: {e}")
                await asyncio.sleep(random.uniform(3, 6))
                continue

            if stop_flag:
                break

        await context.close()
        await browser.close()

    # combine & export
    if all_rows:
        df_all = pd.concat(all_rows, ignore_index=True).drop_duplicates(subset=["url"], keep="first")
        df_all.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"\n[success] Wrote {len(df_all)} listings → {out_csv}")
    else:
        print("\n[warn] No listings scraped in total.")

    if timings:
        pd.DataFrame(timings).to_excel(OUT_DIR / f"timings_{purpose}_{prop_type}.xlsx", index=False)
        print("[success] Wrote timings file.")

    print(f"[end] {datetime.now():%Y-%m-%d %H:%M:%S}")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EdgeProp Singapore property scraper")
    parser.add_argument("--purpose", required=True, choices=["sale", "rental"])
    parser.add_argument("--type", required=True, choices=["hdb", "condo", "landed"])
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    args = parser.parse_args()

    asyncio.run(scrape_edgeprop(args.purpose, args.type, args.max_pages, args.headless))
