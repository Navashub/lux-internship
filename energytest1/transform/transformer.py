"""
Data Transformation Script
Transforms extracted Africa Energy data to required MongoDB schema
Schema: ["country", "country_serial", "metric", "unit", "sector", "sub_sector", 
         "sub_sub_sector", "source_link", "source", "2000", "2001", ..., "2024"]
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os


class EnergyDataTransformer:
    def __init__(self, input_file):
        self.input_file = input_file
        self.df = None
        self.transformed_df = None
        
        # Country serial numbers (alphabetical order)
        self.country_mapping = {}
        
    def load_data(self):
        """Load the extracted CSV data"""
        print(f"[1/5] Loading data from: {self.input_file}")
        self.df = pd.read_csv(self.input_file)
        print(f"      Loaded {len(self.df)} rows, {len(self.df.columns)} columns")
        return self.df
    
    def create_country_mapping(self):
        """Create country serial numbers (1-54 alphabetically)"""
        print(f"\n[2/5] Creating country serial mapping...")
        
        if 'Country_Name' in self.df.columns:
            unique_countries = sorted(self.df['Country_Name'].unique())
            self.country_mapping = {country: idx + 1 for idx, country in enumerate(unique_countries)}
            print(f"      Mapped {len(unique_countries)} countries")
        else:
            print("      WARNING: Country_Name column not found")
        
        return self.country_mapping
    
    def transform_to_schema(self):
        """Transform data to required schema"""
        print(f"\n[3/5] Transforming to required schema...")
        
        records = []
        
        for idx, row in self.df.iterrows():
            # Extract base information
            country_name = row.get('Country_Name', 'Unknown')
            country_serial = self.country_mapping.get(country_name, 0)
            
            # Determine metric based on available data
            if pd.notna(row.get('Title')):
                metric = row.get('Title', 'Unknown Metric')
            else:
                metric = 'Energy Project Data'
            
            # Create the record with required schema
            record = {
                'country': country_name,
                'country_serial': country_serial,
                'metric': metric,
                'unit': row.get('Commitment in UA', 'UA'),  # Use commitment as unit
                'sector': row.get('Sector', 'Energy'),
                'sub_sector': row.get('Sovereign / Non-Sovereign', 'Not Specified'),
                'sub_sub_sector': row.get('Status', None),
                'source_link': row.get('Source_Link', 'https://africa-energy-portal.org/'),
                'source': row.get('Source', 'Africa Energy Portal'),
            }
            
            # Add year columns (2000-2024) - all empty for now as we don't have historical data
            for year in range(2000, 2025):
                record[str(year)] = None
            
            # If we have a signature date, we can put the commitment value in that year
            if pd.notna(row.get('Signature Date')) and pd.notna(row.get('Commitment in UA')):
                try:
                    sig_date = pd.to_datetime(row.get('Signature Date'))
                    year = sig_date.year
                    if 2000 <= year <= 2024:
                        record[str(year)] = row.get('Commitment in UA')
                except:
                    pass
            
            records.append(record)
        
        self.transformed_df = pd.DataFrame(records)
        print(f"      Created {len(records)} records")
        print(f"      Columns: {len(self.transformed_df.columns)}")
        
        return self.transformed_df
    
    def deduplicate_records(self):
        """Remove duplicate records"""
        print(f"\n[4/5] Removing duplicates...")
        
        initial_count = len(self.transformed_df)
        
        # Remove exact duplicates
        self.transformed_df = self.transformed_df.drop_duplicates()
        
        # Group by country and metric to consolidate
        # Keep the first occurrence of each country-metric combination
        self.transformed_df = self.transformed_df.drop_duplicates(
            subset=['country', 'metric'], keep='first'
        )
        
        final_count = len(self.transformed_df)
        removed = initial_count - final_count
        
        print(f"      Removed {removed} duplicates")
        print(f"      Final records: {final_count}")
        
        return self.transformed_df
    
    def save_transformed_data(self, output_file):
        """Save transformed data to CSV"""
        print(f"\n[5/5] Saving transformed data...")
        
        # Ensure column order matches required schema
        base_cols = ['country', 'country_serial', 'metric', 'unit', 'sector', 
                     'sub_sector', 'sub_sub_sector', 'source_link', 'source']
        year_cols = [str(year) for year in range(2000, 2025)]
        all_cols = base_cols + year_cols
        
        # Reorder columns
        self.transformed_df = self.transformed_df[all_cols]
        
        # Save to CSV
        self.transformed_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"      Saved to: {output_file}")
        print(f"      Total rows: {len(self.transformed_df)}")
        print(f"      Total columns: {len(self.transformed_df.columns)}")
        
        return output_file
    
    def generate_summary(self):
        """Generate transformation summary"""
        print(f"\n{'='*80}")
        print("TRANSFORMATION SUMMARY")
        print(f"{'='*80}")
        
        print(f"\nCountries: {self.transformed_df['country'].nunique()}")
        print(f"Unique metrics: {self.transformed_df['metric'].nunique()}")
        print(f"Sectors: {self.transformed_df['sector'].nunique()}")
        
        print(f"\nSample transformed data:")
        print(self.transformed_df[['country', 'country_serial', 'metric', 'sector', 'source']].head(10))
        
        print(f"\nYear columns coverage:")
        year_cols = [str(year) for year in range(2000, 2025)]
        for year in [2000, 2010, 2020, 2022, 2024]:
            non_null = self.transformed_df[str(year)].notna().sum()
            print(f"  {year}: {non_null} non-null values")
        
        return True


def main():
    """Main transformation execution"""
    print("\n" + "="*80)
    print("AFRICA ENERGY DATA TRANSFORMATION")
    print("="*80)
    
    # Input file
    input_file = "africa_energy_complete_20251006_202004.csv"
    
    # Output file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"africa_energy_transformed_{timestamp}.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"\n[ERROR] Input file not found: {input_file}")
        print("Please make sure the extracted data file is in the current directory.")
        return False
    
    # Create transformer
    transformer = EnergyDataTransformer(input_file)
    
    try:
        # Execute transformation pipeline
        transformer.load_data()
        transformer.create_country_mapping()
        transformer.transform_to_schema()
        transformer.deduplicate_records()
        transformer.save_transformed_data(output_file)
        transformer.generate_summary()
        
        print(f"\n{'='*80}")
        print("TRANSFORMATION COMPLETED SUCCESSFULLY!")
        print(f"{'='*80}")
        print(f"\nTransformed file: {output_file}")
        print(f"\nReady for MongoDB loading!")
        
        print(f"\nNote: Year columns (2000-2024) are mostly empty because the")
        print(f"      extracted data is project information, not historical metrics.")
        print(f"      For complete historical data, consider extracting from:")
        print(f"      - World Bank API")
        print(f"      - IEA databases")
        print(f"      - Chart data from country pages")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    main()
