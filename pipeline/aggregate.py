import pandas as pd
import glob
import os
import re
import argparse
import sqlite3

def get_latest_file(directory, pattern):
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def clean_price(price_str):
    if pd.isna(price_str):
        return None
    # Remove 'S$', 'psf', ',', '/mo', etc. to get a raw number
    clean = str(price_str).lower().replace(',', '').replace('s$', '').replace('$', '')
    clean = clean.replace('psf', '').replace('/mo', '').strip()
    try:
        # Extract first numeric sequence if mixed
        import re
        match = re.search(r'(\d+(\.\d+)?)', clean)
        if match:
            return float(match.group(1))
        return float(clean)
    except ValueError:
        return None

def clean_sqft(sqft_str):
    if pd.isna(sqft_str):
        return None
    # Remove 'sqft', commas, clean whitespace
    clean = str(sqft_str).lower().replace('sqft', '').replace(',', '').strip()
    try:
        return float(clean)
    except ValueError:
        return None

def clean_tenure(tenure_str):
    if pd.isna(tenure_str):
        return None
    t = str(tenure_str).lower().strip()
    if '99' in t or 'leasehold' in t:
        return 'Leasehold 99'
    if 'freehold' in t or t == 'f':
        return 'Freehold'
    if '999' in t:
        return 'Leasehold 999'
    return tenure_str

def clean_property_type(pt_str):
    if pd.isna(pt_str):
        return None
    p = str(pt_str).lower().strip()
    
    # 1. Condominium
    if 'condo' in p or 'apartment' in p or 'walk-up' in p or 'cluster house' in p:
        return 'Condominium'
        
    # 2. HDB
    # Catching explicit "HDB", SRX room-type strings ("2 Room", "3 Room", "4 Room", "5 Room"),
    # "Executive", "Studio Apartment", "Jumbo", and generic "Residential" that isn't land.
    # Note: re.match catches 'X room', re.search catches things like 'hdb 4 room'
    if 'hdb' in p or re.search(r'\d+\s*-?\s*room', p) or p in ('executive', 'studio apartment', 'jumbo'):
        return 'HDB'
    if 'residential' in p and 'land' not in p:
        return 'HDB'
        
    # 3. Landed
    if 'landed' in p or 'house' in p or 'terrace' in p or 'detached' in p or 'semi-d' in p or 'bungalow' in p or 'townhouse' in p:
        if 'good class' in p or 'gcb' in p:
            return 'Good Class Bungalow'
        return 'Landed'
        
    # Fallback to Title Case if we really can't figure it out
    return pt_str.title()

def clean_display_string(s):
    if pd.isna(s):
        return None
    # Fix "S$ S$" -> "S$"
    val = str(s)
    val = val.replace("S$ S$", "S$")
    val = val.replace("psf psf", "psf")
    # Generic cleanup of double spaces
    while "  " in val:
        val = val.replace("  ", " ")
    return val.strip()

def normalize_propertyguru(df):
    
    # Generate numeric columns from the existing formatted ones
    df['price'] = df['price_pretty'].apply(clean_price)
    df['psf'] = df['price_psf'].apply(clean_price)

    # Clean text fields
    df['property_type'] = df['property_type'].apply(clean_property_type)
    df['tenure'] = df['tenure'].apply(clean_tenure)
    
    # Clean display strings for duplicates
    df['price_pretty'] = df['price_pretty'].apply(clean_display_string)
    df['price_psf'] = df['price_psf'].apply(clean_display_string)
    
    rename_map = {
        'localizedTitle': 'title',
        'fullAddress': 'address',
        'price_pretty': 'display_price',
        'area_sqft': 'sqft',
        'price_psf': 'display_psf',
        'url_path': 'url',
        'ID': 'id',
        'recency_text': 'posted_date',
        'agent_url_path': 'agent_url',
        'nearbyText': 'nearby_text',
        'CEA': 'cea',
        'agent_description': 'description' # PG's 'agent_description' often contains listing details
    }
    df = df.rename(columns=rename_map)
    
    # Prepend domain to URL if it's a relative path
    def fix_pg_url(u):
        if pd.isna(u) or u == '':
            return None
        u = str(u).strip()
        if u.startswith('http'):
            return u
        # Remove leading slash if present
        if u.startswith('/'):
            u = u[1:]
        return f"https://www.propertyguru.com.sg/{u}"
    
    df['url'] = df['url'].apply(fix_pg_url)

    df['source'] = 'propertyguru'
    return df

def normalize_99co(df):
    
    df['price_numeric'] = df['display_price'].apply(clean_price)
    df['psf_numeric'] = df['psf'].apply(clean_price)
    
    if 'prop_type' in df.columns:
        df['prop_type'] = df['prop_type'].apply(clean_property_type)
    
    if 'tenure' in df.columns:
        df['tenure'] = df['tenure'].apply(clean_tenure)
    
    # Map new fields from merged scraper (from skwips)
    rename_map = {
        'display_price': 'display_price',
        'psf': 'display_psf',
        'prop_type': 'property_type',
        'recency_text': 'posted_date',  # NEW from skwips
        'agent_photo': 'agent_photo_url',  # NEW from skwips
    }
    df = df.rename(columns=rename_map)

    # Restore numeric columns
    df['price'] = df['price_numeric']
    df['psf'] = df['psf_numeric']
    df = df.drop(columns=['price_numeric', 'psf_numeric'])
    
    df['source'] = '99co'
    df['buy_rent'] = df['purpose'].apply(lambda x: 'property-for-sale' if 'sale' in str(x).lower() else 'property-for-rent')
    
    # Use URL from scraper if available (NEW: now extracted directly)
    def synth_url(row):
        if pd.notna(row.get('url')) and str(row.get('url')).startswith('http'): 
            return row['url']
        # Create a deterministic hash based on title/address
        import hashlib
        core = f"{str(row.get('title'))}-{str(row.get('address'))}"
        h = hashlib.md5(core.encode('utf-8')).hexdigest()[:10]
        slug = str(row.get('title', 'listing')).replace(' ', '-').lower()
        return f"https://www.99.co/singapore/sale/property/{slug}-{h}"

    df['url'] = df.apply(synth_url, axis=1)
    
    needed_cols = ['id', 'title', 'address', 'sqft', 'beds', 'baths', 'built_year', 'nearby_text', 'url', 'posted_date', 'agent_id', 'agent_description', 'agent_url', 'cea', 'mobile', 'rating', 'created_at', 'updated_at', 'first_seen_at', 'is_active', 'description', 'agent_photo_url']
    for col in needed_cols:
         if col not in df.columns:
            df[col] = None
            
    return df

def normalize_edgeprop(df):
    # Edgeprop columns: title,price,beds,baths,district,psf,built_year,size_sqft,tenure,address,agent,url,listing_id,image,details,raw_text,purpose,prop_type
    
    # Store original string as display_price BEFORE cleaning
    df['display_price'] = df['price']
    df['price'] = df['display_price'].apply(clean_price)
    
    # Same for psf
    df['display_psf'] = df['psf']
    df['psf'] = df['display_psf'].apply(clean_price) 
    
    # Normalize classification
    df['property_type'] = df['prop_type'].apply(clean_property_type)
    df['tenure'] = df['tenure'].apply(clean_tenure)
    
    # Map fields including new ones from JSON parsing (from skwips)
    rename_map = {
        'title': 'title',
        'address': 'address',
        'size_sqft': 'sqft',
        'built_year': 'built_year',
        'listing_id': 'id',
        'url': 'url',
        'agent': 'agent_name',
        'agent_photo': 'agent_photo_url',  # NEW from skwips
        'agent_cea': 'cea',  # NEW from skwips
        'agent_mobile': 'mobile',  # NEW from skwips
        'agency': 'agent_description',  # NEW from skwips
        'district': 'nearby_text',  # NEW from skwips
        'recency_text': 'posted_date',  # NEW from skwips
        'details': 'description'
    }
    df = df.rename(columns=rename_map)
    
    df['source'] = 'edgeprop'
    df['buy_rent'] = df['purpose'].apply(lambda x: 'property-for-sale' if 'sale' in str(x).lower() else 'property-for-rent')
    
    # Fill missing columns
    needed_cols = ['nearby_text', 'posted_date', 'agent_id', 'agent_description', 'agent_url', 'cea', 'mobile', 'rating', 'created_at', 'updated_at', 'first_seen_at', 'is_active', 'agent_photo_url']
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None
            
    # Ensure dropped internal cols
    cols_to_drop = ['prop_type', 'purpose']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
            
    return df

def normalize_srx(df):
    
    df['price_numeric'] = df['price'].apply(clean_price)
    df['psf_numeric'] = df['psf'].apply(clean_price)
    
    if 'prop_type' in df.columns:
        df['prop_type'] = df['prop_type'].apply(clean_property_type)
        
    if 'tenure' in df.columns:
        df['tenure'] = df['tenure'].apply(clean_tenure) # 'leasehold-99' -> 'Leasehold 99'
    
    # Drop the short 'address' field (listing description) before renaming full_address → address
    if 'full_address' in df.columns and 'address' in df.columns:
        df = df.drop(columns=['address'])

    # Map fields including new ones from merged scraper (from skwips)
    rename_map = {
        'listing_id': 'id',
        'title': 'title',
        'price': 'display_price',
        'size_sqft': 'sqft',
        'psf': 'display_psf',
        'url': 'url',
        'agent_name': 'agent_name',
        'agent_phone_full': 'mobile',
        'agent_user_id': 'agent_id',  # NEW from skwips
        'agent_photo': 'agent_photo_url',  # NEW from skwips
        'agent_profile': 'agent_url',  # Was already there
        'prop_type': 'property_type',
        'posted_age': 'posted_date',
        'full_address': 'address',  # NEW from skwips - now have better address
        'town': 'nearby_text',  # Map town to nearby_text
        'built_year': 'built_year',  # built year from scraper
    }
    df = df.rename(columns=rename_map)
    
    # Use full_address if available, otherwise fallback to title
    if 'address' not in df.columns or df['address'].isna().all():
        df['address'] = df['title']
    else:
        df['address'] = df['address'].fillna(df['title'])
    
    df['price'] = df['price_numeric']
    df['psf'] = df['psf_numeric']
    df = df.drop(columns=['price_numeric', 'psf_numeric'])

    df['source'] = 'srx'
    df['buy_rent'] = df['purpose'].apply(lambda x: 'property-for-sale' if 'sale' in str(x).lower() else 'property-for-rent')
    
    # Fill missing columns
    needed_cols = ['nearby_text', 'agent_id', 'agent_description', 'agent_url', 'cea', 'rating', 'created_at', 'updated_at', 'first_seen_at', 'is_active', 'description', 'agent_photo_url', 'built_year']
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None
            
    return df

def save_to_sqlite(df, db_path):
    """Save DataFrame to SQLite database."""
    conn = sqlite3.connect(db_path)
    # Drop existing table and recreate
    df.to_sql('listings', conn, if_exists='replace', index=False)
    
    # Get row count for verification
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM listings")
    count = cursor.fetchone()[0]
    conn.close()
    
    return count


def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description='Aggregate listings from multiple sources')
    parser.add_argument('--csv', action='store_true', help='Also output CSV file (for debugging)')
    args = parser.parse_args()
    
    # Base paths
    # Go up one level from 'pipeline' to project root, then to 'data'
    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
    
    # 1. PropertyGuru (Target Schema)
    pg_export_dir = os.path.join(base_dir, 'propertyguru', 'export')
    # Using the specific latest file noted or finding latest
    pg_file = get_latest_file(pg_export_dir, 'propertyguru_export_*.csv')
    if not pg_file:
         print("PropertyGuru export not found.")
         return
    print(f"Loading PropertyGuru: {pg_file}")
    df_pg = pd.read_csv(pg_file)
    df_pg = normalize_propertyguru(df_pg)
    print("DEBUG: PG Columns after norm:", df_pg.columns.tolist())
    
    all_dfs = [df_pg]
    
    # 2. 99.co
    co_file = os.path.join(base_dir, '99co', 'sale_listings.csv')
    if os.path.exists(co_file):
        print(f"Loading 99.co: {co_file}")
        df_99 = pd.read_csv(co_file)
        df_99 = normalize_99co(df_99)
        all_dfs.append(df_99)
    else:
        print(f"Skipping 99.co: {co_file} not found")
        df_99 = pd.DataFrame()
    
    # 3. EdgeProp
    ep_file = os.path.join(base_dir, 'edgeprop', 'edgeprop_sale_condo.csv')
    if os.path.exists(ep_file):
        print(f"Loading EdgeProp: {ep_file}")
        df_ep = pd.read_csv(ep_file)
        df_ep = normalize_edgeprop(df_ep)
        all_dfs.append(df_ep)
    else:
        print(f"Skipping EdgeProp: {ep_file} not found")
        df_ep = pd.DataFrame()
    
    # 4. SRX
    srx_file = os.path.join(base_dir, 'srx', 'rent_sale_all_towns.csv')
    if os.path.exists(srx_file):
        print(f"Loading SRX: {srx_file}")
        df_srx = pd.read_csv(srx_file)
        df_srx = normalize_srx(df_srx)
        all_dfs.append(df_srx)
    else:
        print(f"Skipping SRX: {srx_file} not found")
        df_srx = pd.DataFrame()
    
    # Concatenate
    # Use headers from PropertyGuru (now updated)
    target_columns = df_pg.columns.tolist()
    # Ensure source is there
    if 'source' not in target_columns:
        target_columns.append('source')
    
    # all_dfs is already populated with the dataframes that exist
    
    normalized_dfs = []
    for df in all_dfs:
        # Align columns
        # Reindex checks for matching columns and fills others with NaN
        df_sub = df.reindex(columns=target_columns)
        normalized_dfs.append(df_sub)
        
    final_df = pd.concat(normalized_dfs, ignore_index=True)
    
    # Deduplication Logic
    # Normalize address — fall back to title (project name) when address is null.
    # Title is 100% filled in PG; address is 95.5% filled.
    # This prevents null-address rows collapsing into the same "_beds_baths_sqft" bucket.
    final_df['norm_beds'] = final_df['beds'].fillna('0').astype(str).str.extract(r'(\d+)').fillna(0).astype(int)
    final_df['norm_baths'] = final_df['baths'].fillna('0').astype(str).str.extract(r'(\d+)').fillna(0).astype(int)
    final_df['norm_sqft'] = final_df['sqft'].apply(clean_sqft).fillna(0).astype(int)

    # Use address; fall back to title when address is missing
    addr_or_title = final_df['address'].fillna('').astype(str).str.strip()
    title_fallback = final_df['title'].fillna('').astype(str).str.strip()
    final_df['norm_addr'] = addr_or_title.where(addr_or_title != '', title_fallback) \
                                          .str.lower() \
                                          .str.replace(r'[^a-z0-9]', '', regex=True)

    # Create composite key
    final_df['dedup_key'] = (
        final_df['norm_addr'] + '_' +
        final_df['norm_beds'].astype(str) + '_' +
        final_df['norm_baths'].astype(str) + '_' +
        final_df['norm_sqft'].astype(str)
    )

    # --- UNIFICATION: Force display_price to PropertyGuru standard ---
    def standard_format_price(row):
        p = row.get('price')
        if pd.isna(p) or p == '':
            return row.get('display_price') # Fallback
        
        try:
            val = float(p)
            # Format: "S$ 1,234"
            base = "S$ {:,.0f}".format(val)
            
            # Check for rent
            buy_rent = str(row.get('buy_rent', '')).lower()
            if 'rent' in buy_rent:
                return base + " /mo"
            return base
        except:
            # Fallback text
            raw = str(row.get('display_price', ''))
            if raw.strip().startswith('$') and not raw.strip().startswith('S$'):
                raw = raw.replace('$', 'S$ ', 1) # simple replace
            return raw

    final_df['display_price'] = final_df.apply(standard_format_price, axis=1)

    def standard_format_psf(row):
        p = row.get('psf')
        # If numeric psf is present, FORCE the format
        if pd.notna(p) and p != '':
            try:
                val = float(p)
                return "S$ {:,.2f} psf".format(val)
            except:
                pass # Fall through to string cleanup
                
        # Fallback / Text cleanup
        raw = str(row.get('display_psf', ''))
        
        # Filter out junk
        if '未知' in raw or raw.lower() == 'nan' or raw.strip() == '':
            return None
            
        # Clean
        while 'S$ S$' in raw:
            raw = raw.replace('S$ S$', 'S$')
        while 'psf psf' in raw:
            raw = raw.replace('psf psf', 'psf')
            
        # Fix leading $
        if raw.strip().startswith('$') and not raw.strip().startswith('S$'):
             raw = raw.replace('$', 'S$ ', 1)
             
        # Final check: Must start with S$
        if not raw.strip().startswith('S$'):
            return None 
            
        return raw

    final_df['display_psf'] = final_df.apply(standard_format_psf, axis=1)
    # -----------------------------------------------------------------

    print(f"Total rows before dedupe: {len(final_df)}")
    
    # Sort by source priority (propertyguru first) or recency
    # We'll make 'propertyguru' source a sort key using categorical
    # Using 'propertyguru' as the first category ensures it is prioritized if we keep='first' after sorting
    final_df['source'] = pd.Categorical(final_df['source'], categories=['propertyguru', '99co', 'edgeprop', 'srx'], ordered=True)
    
    # Sort: Source (asc). This puts 'propertyguru' entries first.
    final_df = final_df.sort_values('source')
    
    # Drop duplicates
    final_df = final_df.drop_duplicates(subset=['dedup_key'], keep='first')
    
    print(f"Total rows after dedupe: {len(final_df)}")
    
    # Clean up aux columns
    final_df = final_df.drop(columns=['norm_beds', 'norm_baths', 'norm_sqft', 'norm_addr', 'dedup_key'])
    
    # Update logic: ensure we are renaming based on ACTUAL columns
    print("Final columns:", final_df.columns.tolist())
    
    # === OUTPUT ===
    # Primary: SQLite
    sqlite_path = os.path.join(base_dir, 'aggregated.db')
    count = save_to_sqlite(final_df, sqlite_path)
    print(f"Saved {count} listings to SQLite: {sqlite_path}")
    
    # Optional: CSV (for debugging)
    if args.csv:
        csv_path = os.path.join(base_dir, 'aggregated_listings.csv')
        final_df.to_csv(csv_path, index=False)
        print(f"Saved CSV (debug): {csv_path}")

if __name__ == "__main__":
    main()

