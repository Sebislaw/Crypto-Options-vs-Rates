import duckdb
from config import settings
import pandas as pd

def verify_data():
    print(f"Verifying data in {settings.DATA_DIR}...")
    
    # DuckDB can read parquet files directly using glob patterns
    parquet_pattern = str(settings.DATA_DIR / "**" / "*.parquet")
    
    try:
        # Check if any files exist first
        import glob
        files = glob.glob(parquet_pattern, recursive=True)
        if not files:
            print("No parquet files found yet.")
            return

        print(f"Found {len(files)} parquet files.")
        
        # Query using DuckDB
        con = duckdb.connect()
        
        # Count total rows
        count = con.execute(f"SELECT count(*) FROM '{parquet_pattern}'").fetchone()[0]
        print(f"Total records: {count}")
        
        # Show sample
        print("\nSample data (top 5):")
        df = con.execute(f"SELECT * FROM '{parquet_pattern}' LIMIT 5").df()
        print(df)
        
        # Check symbols
        print("\nSymbols found:")
        symbols = con.execute(f"SELECT DISTINCT symbol FROM '{parquet_pattern}'").df()
        print(symbols)
        
        # Check for recent data (snapshots)
        print("\nChecking for recent snapshots...")
        try:
            # Check if snapshot_time column exists
            columns = con.execute(f"DESCRIBE SELECT * FROM '{parquet_pattern}' LIMIT 1").df()
            if 'snapshot_time' in columns['column_name'].values:
                recent = con.execute(f"SELECT * FROM '{parquet_pattern}' WHERE snapshot_time IS NOT NULL ORDER BY snapshot_time DESC LIMIT 5").df()
                print("Recent snapshots found:")
                print(recent[['symbol', 'timestamp', 'snapshot_time', 'close']])
            else:
                print("No 'snapshot_time' column found. Data might be from old schema.")
        except Exception as e:
            print(f"Snapshot check failed: {e}")
            
    except Exception as e:
        print(f"Verification failed: {e}")

if __name__ == "__main__":
    verify_data()
