"""
Automated script to run MongoDB loader without interactive prompts
"""
import sys
import os

# Add load directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'load'))

from load.mongodb_loader import MongoDBLoader
from dotenv import load_dotenv

load_dotenv()

def run_loader():
    print("\n" + "="*80)
    print("MONGODB DATA LOADER - AFRICA ENERGY DATA (AUTOMATED)")
    print("="*80)
    
    # Configuration
    csv_file = "africa_energy_transformed_20251006_204713.csv"
    
    # Check if CSV exists
    if not os.path.exists(csv_file):
        print(f"\n[ERROR] Transformed data file not found: {csv_file}")
        print("Please run the transformation script first.")
        return False
    
    # Use defaults from the class
    print(f"\nConfiguration:")
    print(f"  Database: energyd2")
    print(f"  Collection: test")
    print(f"  Clear existing data: No")
    
    print(f"\n{'='*80}")
    
    # Create loader with defaults (will use MongoDB Atlas connection)
    loader = MongoDBLoader()
    
    try:
        # Connect to MongoDB
        if not loader.connect():
            return False
        
        # Load CSV
        df = loader.load_csv(csv_file)
        if df is None:
            return False
        
        # Prepare documents
        documents = loader.prepare_documents(df)
        
        # Load data (don't clear existing)
        if not loader.load_data(documents, clear_existing=False):
            return False
        
        # Create indexes
        loader.create_indexes()
        
        # Verify
        loader.verify_load()
        
        print(f"\n{'='*80}")
        print("DATA LOADING COMPLETED SUCCESSFULLY!")
        print(f"{'='*80}")
        print(f"\nDatabase: energyd2")
        print(f"Collection: test")
        print(f"Total documents: {len(documents)}")
        
        return True
        
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        loader.close()

if __name__ == "__main__":
    success = run_loader()
    sys.exit(0 if success else 1)
