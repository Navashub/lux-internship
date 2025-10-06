"""
Load data to MongoDB (long format)
"""
import sys
import os
from dotenv import load_dotenv
import glob

load_dotenv()

# Import from same directory
from mongodb_loader import MongoDBLoader

def load_data():
    print("\n" + "="*80)
    print("MONGODB DATA LOADER - LONG FORMAT DATA")
    print("="*80)
    
    # Get the project root directory (parent of load/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Look for the latest long format CSV in project root
    csv_pattern = os.path.join(project_root, "africa_energy_long_format_*.csv")
    csv_files = sorted(glob.glob(csv_pattern), reverse=True)
    
    if not csv_files:
        print(f"\n[ERROR] No long format data file found in: {project_root}")
        print("Please run transform/transform_to_long_format.py first.")
        return False
    
    # Use the latest file
    csv_file = csv_files[0]
    
    print(f"\nConfiguration:")
    print(f"  Input file: {os.path.basename(csv_file)}")
    print(f"  Database: energyd2")
    print(f"  Collection: test")
    print(f"  Clear existing data: YES")
    
    print(f"\n{'='*80}")
    
    # Create loader with defaults
    loader = MongoDBLoader()
    
    try:
        # Connect to MongoDB
        if not loader.connect():
            return False
        
        # Load CSV
        df = loader.load_csv(csv_file)
        if df is None:
            return False
        
        print(f"\n      Data structure:")
        print(f"      Columns: {df.columns.tolist()}")
        print(f"\n      Sample record:")
        print(f"      {df.iloc[0].to_dict()}")
        
        # Prepare documents
        documents = loader.prepare_documents(df)
        
        # Load data (CLEAR existing data)
        if not loader.load_data(documents, clear_existing=True):
            return False
        
        # Create indexes (including year)
        print(f"\n[5/5] Creating indexes...")
        
        try:
            from pymongo import ASCENDING
            
            # Index on country
            loader.collection.create_index([("country", ASCENDING)])
            print(f"      Created index: country")
            
            # Index on year
            loader.collection.create_index([("year", ASCENDING)])
            print(f"      Created index: year")
            
            # Compound index on country and year
            loader.collection.create_index([
                ("country", ASCENDING),
                ("year", ASCENDING)
            ])
            print(f"      Created index: country + year")
            
            # Index on sector
            loader.collection.create_index([("sector", ASCENDING)])
            print(f"      Created index: sector")
            
            print(f"      [OK] All indexes created")
            
        except Exception as e:
            print(f"      [WARNING] Index creation failed: {e}")
        
        # Custom verification for long format
        print(f"\n{'='*80}")
        print("VERIFICATION")
        print(f"{'='*80}")
        
        total_docs = loader.collection.count_documents({})
        print(f"\nTotal documents: {total_docs}")
        
        unique_countries = len(loader.collection.distinct("country"))
        print(f"Unique countries: {unique_countries}")
        
        unique_years = sorted(loader.collection.distinct("year"))
        print(f"Year range: {unique_years}")
        
        print(f"\nSample documents (first 3):")
        for idx, doc in enumerate(loader.collection.find().limit(3), 1):
            print(f"\n  Document {idx}:")
            print(f"    Country: {doc.get('country')}")
            print(f"    Year: {doc.get('year')}")
            print(f"    Metric: {doc.get('metric', 'N/A')[:60]}...")
            print(f"    Value: {doc.get('value')}")
            print(f"    Sector: {doc.get('sector')}")
        
        print(f"\n{'='*80}")
        print("DATA LOADING COMPLETED SUCCESSFULLY!")
        print(f"{'='*80}")
        print(f"\nDatabase: energyd2")
        print(f"Collection: test")
        print(f"Total documents: {len(documents)}")
        print(f"\nData is now in long format - each document represents:")
        print(f"  country + year + metric + value")
        print(f"\nExample query:")
        print(f'  db.test.find({{"country": "Nigeria", "year": 2018}})')
        
        return True
        
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        loader.close()

if __name__ == "__main__":
    success = load_data()
    sys.exit(0 if success else 1)
