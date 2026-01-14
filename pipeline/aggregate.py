import pandas as pd
import glob
import os
import re

def get_latest_file(directory, pattern):
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def clean_price(price_str):
    if pd.isna(price_str):
        return None
    # Remove 'S$', ',', '/mo', etc. to get a raw number if possible, or keep as consistent string
    clean = str(price_str).replace(',', '').replace('S$', '').replace('$', '').split('/')[0].strip()
    try:
        return float(clean)
    except ValueError:
        return None

def clean_sqft(sqft_str):
    if pd.isna(sqft_str):
        return None
    clean = str(sqft_str).lower().replace('sqft', '').replace(',', '').strip()
    try:
        return float(clean)
    except ValueError:
        return None

def normalize_propertyguru(df):
    
    # Generate numeric columns from the existing formatted ones
    df['price'] = df['price_pretty'].apply(clean_price)
    df['psf'] = df['price_psf'].apply(clean_price)
    
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
        'CEA': 'cea'
    }
    df = df.rename(columns=rename_map)
    df['source'] = 'propertyguru'
    return df

def normalize_99co(df):
    # Old Target: ID,localizedTitle,fullAddress,price_pretty,beds,baths,area_sqft,price_psf,nearbyText,built_year,property_type,tenure,url_path,recency_text...
    # New Target: id, title, address, display_price, price, beds, baths, sqft, display_psf, psf, nearby_text, built_year, property_type, tenure, url, posted_date, agent_id...
    
    df['price'] = df['display_price'].apply(clean_price)
    # psf is strictly numeric or formatted? 99co 'psf' is usually string "S$ x psf". 
    # Let's clean it for a numeric 'psf' and keep original as 'display_psf'
    df['psf'] = df['psf'].apply(clean_price)
    
    rename_map = {
        'title': 'title',
        'address': 'address',
        'display_price': 'display_price',
        'sqft': 'sqft',
        'psf': 'display_psf',
        'prop_type': 'property_type',
        # 'purpose': 'buy_rent' 
    }
    df = df.rename(columns=rename_map)
    
    df['source'] = '99co'
    df['buy_rent'] = df['purpose'].apply(lambda x: 'for-sale' if 'sale' in str(x).lower() else 'for-rent')
    
    # Fill missing new target columns with snake_case
    needed_cols = ['id', 'nearby_text', 'url', 'posted_date', 'agent_id', 'agent_description', 'agent_url', 'cea', 'mobile', 'rating', 'created_at', 'updated_at', 'first_seen_at', 'is_active']
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None
            
    return df

def normalize_edgeprop(df):
    # Edgeprop columns: title,price,beds,baths,district,psf,built_year,size_sqft,tenure,address,agent,url,listing_id,image,details,raw_text,purpose,prop_type
    
    df['price'] = df['price'].apply(clean_price)
    df['psf_numeric'] = df['psf'].apply(clean_price) # temp name to avoid collision if we rename 'psf' to 'display_psf' immediately
    
    rename_map = {
        'title': 'title',
        'address': 'address',
        'price': 'display_price', # existing 'price' is formatted string "$1,720,000"
        'size_sqft': 'sqft',
        'psf': 'display_psf',
        'prop_type': 'property_type',
        'listing_id': 'id',
        'url': 'url',
        'agent': 'agent_name'
    }
    df = df.rename(columns=rename_map)
    df['psf'] = df['psf_numeric']
    df = df.drop(columns=['psf_numeric'])
    
    df['source'] = 'edgeprop'
    df['buy_rent'] = df['purpose'].apply(lambda x: 'for-sale' if 'sale' in str(x).lower() else 'for-rent')
    
    # Fill missing with snake_case
    needed_cols = ['nearby_text', 'posted_date', 'agent_id', 'agent_description', 'agent_url', 'cea', 'mobile', 'rating', 'created_at', 'updated_at', 'first_seen_at', 'is_active']
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None
            
    return df

def normalize_srx(df):
    # SRX columns: listing_id,title,url,price,prop_type,tenure,size_sqft,psf,beds,baths,address,photo,posted_age,posted_epoch,is_sale,project_name,town,postal,built_size_hidden,created_date,expiry_date,lat_long,agent_name,agent_profile,agent_photo,agent_phone_masked,agent_phone_full,agent_user_id,agency_id,agent_call,agent_whatsapp,town_id,purpose,page
    
    df['price_numeric'] = df['price'].apply(clean_price) # srx 'price' is "$468,000"
    df['psf_numeric'] = df['psf'].apply(clean_price)
    
    rename_map = {
        'listing_id': 'id',
        'title': 'title',
        'address': 'address',
        'price': 'display_price',
        'size_sqft': 'sqft',
        'psf': 'display_psf',
        'url': 'url',
        'agent_name': 'agent_name',
        'agent_phone_full': 'mobile',
        'prop_type': 'property_type',
        'posted_age': 'posted_date' # maybe? "Listed on ..." vs "2h ago"
    }
    df = df.rename(columns=rename_map)
    
    df['price'] = df['price_numeric']
    df['psf'] = df['psf_numeric']
    df = df.drop(columns=['price_numeric', 'psf_numeric'])

    df['source'] = 'srx'
    df['buy_rent'] = df['purpose'].apply(lambda x: 'for-sale' if 'sale' in str(x).lower() else 'for-rent')
    
    needed_cols = ['nearby_text', 'agent_id', 'agent_description', 'agent_url', 'cea', 'rating', 'created_at', 'updated_at', 'first_seen_at', 'is_active']
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None
            
    return df

def main():
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
    
    # 2. 99.co
    co_file = os.path.join(base_dir, '99co', 'sale_listings.csv')
    print(f"Loading 99.co: {co_file}")
    df_99 = pd.read_csv(co_file)
    df_99 = normalize_99co(df_99)
    
    # 3. EdgeProp
    ep_file = os.path.join(base_dir, 'edgeprop', 'edgeprop_sale_condo.csv')
    print(f"Loading EdgeProp: {ep_file}")
    df_ep = pd.read_csv(ep_file)
    df_ep = normalize_edgeprop(df_ep)
    
    # 4. SRX
    srx_file = os.path.join(base_dir, 'srx', 'srx_listings.csv')
    print(f"Loading SRX: {srx_file}")
    df_srx = pd.read_csv(srx_file)
    df_srx = normalize_srx(df_srx)
    
    # Concatenate
    # Use headers from PropertyGuru (now updated)
    target_columns = df_pg.columns.tolist()
    # Ensure source is there
    if 'source' not in target_columns:
        target_columns.append('source')
    
    all_dfs = [df_pg, df_99, df_ep, df_srx]
    
    normalized_dfs = []
    for df in all_dfs:
        # Align columns
        # Reindex checks for matching columns and fills others with NaN
        df_sub = df.reindex(columns=target_columns)
        normalized_dfs.append(df_sub)
        
    final_df = pd.concat(normalized_dfs, ignore_index=True)
    
    # Deduplication Logic
    # 1. Normalize Address (very basic for now)
    final_df['norm_beds'] = final_df['beds'].fillna('0').astype(str).str.extract(r'(\d+)').fillna(0).astype(int)
    final_df['norm_baths'] = final_df['baths'].fillna('0').astype(str).str.extract(r'(\d+)').fillna(0).astype(int)
    final_df['norm_sqft'] = final_df['sqft'].apply(clean_sqft).fillna(0).astype(int)
    final_df['norm_addr'] = final_df['address'].astype(str).str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
    
    # Create composite key
    final_df['dedup_key'] = (
        final_df['norm_addr'] + '_' + 
        final_df['norm_beds'].astype(str) + '_' + 
        final_df['norm_baths'].astype(str) + '_' + 
        final_df['norm_sqft'].astype(str)
    )
    
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
    
    output_path = os.path.join(base_dir, 'aggregated_listings.csv')
    final_df.to_csv(output_path, index=False)
    print(f"Saved aggregated data to {output_path}")
