# Restoring the Master Databases (HDB & Condos)

This repository includes a database dump `data/master_db_dump.sql` containing the master directories for all HDB Blocks and Condominiums in Singapore.

## How to restore this on your machine:
Warning: Ensure your PostgreSQL server is running locally and you have created a database named `real_estate_fyp`.

Run the following command in your terminal from the root folder:
```bash
psql -U postgres -d real_estate_fyp -f data/master_db_dump.sql
```

Once this finishes running, your local `real_estate_fyp` database will be populated with the 13k+ HDB records and 2.5k Condo records. You can then run the scrapers/ingest pipelines to attach active listings to them!
