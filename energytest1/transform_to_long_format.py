"""
Transform wide format CSV to long format suitable for MongoDB
Each row will become multiple rows - one per year with actual data
"""
import pandas as pd
import numpy as np
from datetime import datetime

def transform_to_long_format(csv_file):
    """Transform wide format to long format"""
    print("="*80)
    print("TRANSFORMING DATA TO LONG FORMAT")
    print("="*80)
    
    # Load the data
    print(f"\n[1/4] Loading CSV file: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"      Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Identify year columns
    year_columns = [col for col in df.columns if col.isdigit() and 2000 <= int(col) <= 2030]
    metadata_columns = [col for col in df.columns if col not in year_columns]
    
    print(f"\n[2/4] Identified columns:")
    print(f"      Metadata columns: {metadata_columns}")
    print(f"      Year columns: {len(year_columns)} years ({year_columns[0]} to {year_columns[-1]})")
    
    # Melt the dataframe
    print(f"\n[3/4] Converting to long format...")
    df_long = pd.melt(
        df,
        id_vars=metadata_columns,
        value_vars=year_columns,
        var_name='year',
        value_name='value'
    )
    
    # Convert year to integer
    df_long['year'] = df_long['year'].astype(int)
    
    # Remove rows with NaN values
    before_count = len(df_long)
    df_long = df_long.dropna(subset=['value'])
    after_count = len(df_long)
    print(f"      Before filtering: {before_count} rows")
    print(f"      After removing NaN values: {after_count} rows")
    print(f"      Removed: {before_count - after_count} rows with no data")
    
    # Sort by country, metric, year
    df_long = df_long.sort_values(['country', 'metric', 'year']).reset_index(drop=True)
    
    print(f"\n[4/4] Final structure:")
    print(f"      Total documents: {len(df_long)}")
    print(f"      Columns: {df_long.columns.tolist()}")
    
    # Show sample
    print(f"\n      Sample records:")
    for idx, row in df_long.head(3).iterrows():
        print(f"        {row['country']} - {row['metric'][:50]}... - {row['year']}: {row['value']}")
    
    return df_long


def main():
    input_file = "africa_energy_transformed_20251006_204713.csv"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"africa_energy_long_format_{timestamp}.csv"
    
    # Transform
    df_long = transform_to_long_format(input_file)
    
    # Save
    print(f"\n{'='*80}")
    print(f"Saving to: {output_file}")
    df_long.to_csv(output_file, index=False)
    print(f"[OK] Saved successfully!")
    
    # Summary statistics
    print(f"\n{'='*80}")
    print("SUMMARY STATISTICS")
    print(f"{'='*80}")
    print(f"Total records: {len(df_long)}")
    print(f"Countries: {df_long['country'].nunique()}")
    print(f"Metrics: {df_long['metric'].nunique()}")
    print(f"Year range: {df_long['year'].min()} to {df_long['year'].max()}")
    print(f"Records with data: {df_long['value'].notna().sum()}")
    
    print(f"\n{'='*80}")
    print("TRANSFORMATION COMPLETED!")
    print(f"{'='*80}")
    print(f"\nOutput file: {output_file}")
    print(f"This file is ready to be loaded into MongoDB")
    
    return output_file


if __name__ == "__main__":
    output_file = main()
