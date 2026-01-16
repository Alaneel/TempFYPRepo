import os
from sqlalchemy import create_engine, text

# Use system user 'alanwang'
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'property_db')
DB_USER = os.getenv('DB_USER', 'alanwang')
DB_PASS = os.getenv('DB_PASS', '')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def verify():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("--- Verification ---")
        
        # Counts
        agents_count = conn.execute(text("SELECT count(*) FROM agents")).scalar()
        listings_count = conn.execute(text("SELECT count(*) FROM listings")).scalar()
        print(f"Agents Count: {agents_count}")
        print(f"Listings Count: {listings_count}")
        
        # Join check
        print("\n--- Sample Listing with Agent ---")
        query = text("""
            SELECT l.title, l.price, l.beds, l.sqft, a.name, a.mobile 
            FROM listings l 
            JOIN agents a ON l.agent_id = a.id 
            WHERE l.price > 0 AND l.beds IS NOT NULL 
            LIMIT 5
        """)
        result = conn.execute(query).fetchall()
        for row in result:
            print(row)
            
        # Agent dedupe check
        print("\n--- Agent Duplication Check ---")
        dupe_agents = conn.execute(text("SELECT name, mobile, count(*) as c FROM agents GROUP BY name, mobile HAVING count(*) > 1")).fetchall()
        if dupe_agents:
            print(f"WARNING: Found {len(dupe_agents)} duplicate agents!")
        else:
            print("No duplicate agents found (by name+mobile).")

if __name__ == '__main__':
    verify()
