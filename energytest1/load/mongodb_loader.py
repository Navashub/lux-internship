"""
MongoDB Data Loader
Loads transformed Africa Energy data into MongoDB collection
"""

import pandas as pd
import pymongo
from pymongo import MongoClient
from datetime import datetime
import os
import sys
from dotenv import load_dotenv

load_dotenv()


class MongoDBLoader:
    def __init__(self, connection_string=None, 
                 database_name="energyd2",
                 collection_name="test"):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection URI
            database_name: Name of the database
            collection_name: Name of the collection
        """
        if connection_string is None:
            mongo_password = os.getenv('MONGO_PASSWORD')
            if not mongo_password:
                raise ValueError("MONGO_PASSWORD not found in .env file")
            connection_string = f"mongodb+srv://navasmuller01_db_user:{mongo_password}@energydb1.ehddpri.mongodb.net/?retryWrites=true&w=majority&appName=energydb1"
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self):
        """Establish connection to MongoDB"""
        print(f"[1/5] Connecting to MongoDB...")
        print(f"      URI: {self.connection_string}")
        
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.server_info()
            print(f"      [OK] Connected successfully!")
            
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            print(f"      Database: {self.database_name}")
            print(f"      Collection: {self.collection_name}")
            
            return True
            
        except Exception as e:
            print(f"      [ERROR] Connection failed: {e}")
            print(f"\n      Troubleshooting:")
            print(f"      1. Make sure MongoDB is installed and running")
            print(f"      2. Check connection string: {self.connection_string}")
            print(f"      3. Install MongoDB: https://www.mongodb.com/try/download/community")
            return False
    
    def load_csv(self, csv_file):
        """Load CSV file into pandas DataFrame"""
        print(f"\n[2/5] Loading CSV file...")
        print(f"      File: {csv_file}")
        
        if not os.path.exists(csv_file):
            print(f"      [ERROR] File not found: {csv_file}")
            return None
        
        df = pd.read_csv(csv_file)
        print(f"      [OK] Loaded {len(df)} rows, {len(df.columns)} columns")
        
        return df
    
    def prepare_documents(self, df):
        """Convert DataFrame to MongoDB documents"""
        print(f"\n[3/5] Preparing documents for MongoDB...")
        
        # Replace NaN with None for MongoDB
        df = df.where(pd.notna(df), None)
        
        # Convert DataFrame to list of dictionaries
        documents = df.to_dict('records')
        
        print(f"      [OK] Prepared {len(documents)} documents")
        
        return documents
    
    def load_data(self, documents, clear_existing=False):
        """Load documents into MongoDB collection"""
        print(f"\n[4/5] Loading data into MongoDB...")
        
        if clear_existing:
            print(f"      Clearing existing data...")
            result = self.collection.delete_many({})
            print(f"      Deleted {result.deleted_count} existing documents")
        
        try:
            # Insert documents
            print(f"      Inserting {len(documents)} documents...")
            result = self.collection.insert_many(documents)
            
            print(f"      [OK] Inserted {len(result.inserted_ids)} documents")
            
            return True
            
        except Exception as e:
            print(f"      [ERROR] Insert failed: {e}")
            return False
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        print(f"\n[5/5] Creating indexes...")
        
        try:
            # Index on country for fast country lookups
            self.collection.create_index([("country", pymongo.ASCENDING)])
            print(f"      Created index: country")
            
            # Index on country_serial
            self.collection.create_index([("country_serial", pymongo.ASCENDING)])
            print(f"      Created index: country_serial")
            
            # Compound index on country and metric
            self.collection.create_index([
                ("country", pymongo.ASCENDING),
                ("metric", pymongo.ASCENDING)
            ])
            print(f"      Created index: country + metric")
            
            # Index on sector
            self.collection.create_index([("sector", pymongo.ASCENDING)])
            print(f"      Created index: sector")
            
            print(f"      [OK] All indexes created")
            
            return True
            
        except Exception as e:
            print(f"      [WARNING] Index creation failed: {e}")
            return False
    
    def verify_load(self):
        """Verify the loaded data"""
        print(f"\n{'='*80}")
        print("VERIFICATION")
        print(f"{'='*80}")
        
        try:
            # Count documents
            total_docs = self.collection.count_documents({})
            print(f"\nTotal documents in collection: {total_docs}")
            
            # Count unique countries
            unique_countries = len(self.collection.distinct("country"))
            print(f"Unique countries: {unique_countries}")
            
            # Count unique metrics
            unique_metrics = len(self.collection.distinct("metric"))
            print(f"Unique metrics: {unique_metrics}")
            
            # Sample documents
            print(f"\nSample documents (first 3):")
            for idx, doc in enumerate(self.collection.find().limit(3), 1):
                print(f"\n  Document {idx}:")
                print(f"    Country: {doc.get('country')}")
                print(f"    Serial: {doc.get('country_serial')}")
                print(f"    Metric: {doc.get('metric')}")
                print(f"    Sector: {doc.get('sector')}")
                print(f"    Source: {doc.get('source')}")
            
            # Check for countries with data
            print(f"\nCountries in database:")
            countries = sorted(self.collection.distinct("country"))
            for i in range(0, len(countries), 10):
                print(f"  {', '.join(countries[i:i+10])}")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Verification failed: {e}")
            return False
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print(f"\n[OK] MongoDB connection closed")


def main():
    """Main execution"""
    print("\n" + "="*80)
    print("MONGODB DATA LOADER - AFRICA ENERGY DATA")
    print("="*80)
    
    # Configuration
    csv_file = "africa_energy_transformed_20251006_204713.csv"
    
    # Check if CSV exists
    if not os.path.exists(csv_file):
        print(f"\n[ERROR] Transformed data file not found: {csv_file}")
        print("Please run the transformation script first.")
        return False
    
    # MongoDB connection settings
    print(f"\nMongoDB Configuration:")
    mongo_password = os.getenv('MONGO_PASSWORD')
    if not mongo_password:
        print("[ERROR] MONGO_PASSWORD not found in .env file")
        return False
    
    default_connection = f"mongodb+srv://navasmuller01_db_user:{mongo_password}@energydb1.ehddpri.mongodb.net/?retryWrites=true&w=majority&appName=energydb1"
    connection_string = input(f"  Connection string [MongoDB Atlas]: ").strip() or default_connection
    database_name = input("  Database name [energyd2]: ").strip() or "energyd2"
    collection_name = input("  Collection name [test]: ").strip() or "test"
    
    clear_existing = input("\n  Clear existing data? (y/n) [n]: ").strip().lower() == 'y'
    
    print(f"\n{'='*80}")
    
    # Create loader
    loader = MongoDBLoader(connection_string, database_name, collection_name)
    
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
        
        # Load data
        if not loader.load_data(documents, clear_existing):
            return False
        
        # Create indexes
        loader.create_indexes()
        
        # Verify
        loader.verify_load()
        
        print(f"\n{'='*80}")
        print("DATA LOADING COMPLETED SUCCESSFULLY!")
        print(f"{'='*80}")
        print(f"\nDatabase: {database_name}")
        print(f"Collection: {collection_name}")
        print(f"Total documents: {len(documents)}")
        
        print(f"\nYou can now query the data using MongoDB!")
        print(f"\nExample queries:")
        print(f"  # Find all data for Nigeria")
        print(f"  db.{collection_name}.find({{\"country\": \"Nigeria\"}})")
        print(f"\n  # Count documents by country")
        print(f"  db.{collection_name}.aggregate([{{\"$group\": {{\"_id\": \"$country\", \"count\": {{\"$sum\": 1}}}}}}])")
        
        return True
        
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        loader.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
