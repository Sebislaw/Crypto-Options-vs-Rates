#!/usr/bin/env python3
"""
HBase Connection Test & Sample Query Script
Purpose: Verify HBase connectivity and query sample results
"""

import sys

try:
    import happybase
except ImportError:
    print("[ERROR] happybase not installed!")
    print("Install with: pip install happybase")
    sys.exit(1)


def test_connection(host='localhost', port=9090):
    """Test HBase connection"""
    try:
        print(f"Connecting to HBase at {host}:{port}...")
        connection = happybase.Connection(host=host, port=port, timeout=10000)
        
        print("✓ Connected successfully!")
        print("\nAvailable tables:")
        tables = connection.tables()
        for table in tables:
            print(f"  - {table.decode('utf-8')}")
        
        return connection
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Is HBase running? Check: echo 'status' | hbase shell")
        print("  2. Is Thrift server running? Check: ps aux | grep ThriftServer")
        print("  3. Start Thrift: hbase-daemon.sh start thrift")
        return None


def query_analytics_table(connection, limit=10):
    """Query sample results from market_analytics table"""
    try:
        table = connection.table('market_analytics')
        
        print(f"\n{'='*80}")
        print(f"Sample Results from market_analytics (limit={limit})")
        print('='*80)
        
        count = 0
        for key, data in table.scan(limit=limit):
            count += 1
            print(f"\nRow Key: {key.decode('utf-8')}")
            print("-" * 80)
            
            # Price data
            if b'analysis:symbol' in data:
                symbol = data[b'analysis:symbol'].decode('utf-8')
                print(f"Symbol: {symbol}")
            
            if b'analysis:timestamp' in data:
                timestamp = data[b'analysis:timestamp'].decode('utf-8')
                print(f"Window: {timestamp}")
            
            # Price movement
            if b'price_data:open' in data and b'price_data:close' in data:
                open_price = float(data[b'price_data:open'].decode('utf-8'))
                close_price = float(data[b'price_data:close'].decode('utf-8'))
                print(f"Price: {open_price:.2f} → {close_price:.2f}")
            
            # Prediction
            if b'bet_data:avg_prob' in data:
                prob = float(data[b'bet_data:avg_prob'].decode('utf-8'))
                print(f"Avg Probability: {prob:.3f}")
            
            # Result
            if b'analysis:result' in data:
                result = data[b'analysis:result'].decode('utf-8')
                print(f"Prediction Result: {result}")
            
            if b'analysis:price_movement' in data:
                movement = float(data[b'analysis:price_movement'].decode('utf-8'))
                print(f"Price Movement: {movement*100:.3f}%")
        
        print(f"\n{'='*80}")
        print(f"Displayed {count} records")
        print('='*80)
        
    except Exception as e:
        print(f"\n[ERROR] Failed to query table: {e}")
        print("\nMake sure the table exists: echo \"describe 'market_analytics'\" | hbase shell")


def get_summary_stats(connection):
    """Get summary statistics from the table"""
    try:
        table = connection.table('market_analytics')
        
        results = {
            'CORRECT_BULL': 0,
            'FAILED_BULL': 0,
            'CORRECT_BEAR': 0,
            'FAILED_BEAR': 0,
            'UNCERTAIN': 0
        }
        
        total = 0
        for key, data in table.scan():
            total += 1
            if b'analysis:result' in data:
                result = data[b'analysis:result'].decode('utf-8')
                if result in results:
                    results[result] += 1
        
        print(f"\n{'='*80}")
        print("Summary Statistics")
        print('='*80)
        print(f"Total Records: {total}")
        print(f"\nPrediction Accuracy:")
        for result_type, count in results.items():
            pct = (count / total * 100) if total > 0 else 0
            print(f"  {result_type:20s}: {count:5d} ({pct:5.1f}%)")
        
        # Calculate overall accuracy
        correct = results['CORRECT_BULL'] + results['CORRECT_BEAR']
        failed = results['FAILED_BULL'] + results['FAILED_BEAR']
        if correct + failed > 0:
            accuracy = correct / (correct + failed) * 100
            print(f"\n  Overall Accuracy: {accuracy:.1f}%")
        
        print('='*80)
        
    except Exception as e:
        print(f"\n[ERROR] Failed to compute statistics: {e}")


def main():
    print("="*80)
    print("HBase Market Analytics Query Tool")
    print("="*80)
    
    # Test connection
    connection = test_connection()
    
    if not connection:
        sys.exit(1)
    
    # Check if table exists
    tables = connection.tables()
    if b'market_analytics' not in tables:
        print("\n[ERROR] Table 'market_analytics' does not exist!")
        print("Create it with: bash batch_layer/hbase/create_table.sh")
        connection.close()
        sys.exit(1)
    
    # Query sample data
    query_analytics_table(connection, limit=5)
    
    # Get statistics
    print("\nComputing summary statistics (this may take a moment)...")
    get_summary_stats(connection)
    
    # Close connection
    connection.close()
    print("\n✓ Connection closed")


if __name__ == "__main__":
    main()
