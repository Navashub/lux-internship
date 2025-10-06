"""
Main ETL Pipeline Runner
Orchestrates the complete Extract, Transform, Load process for Africa Energy Data
"""
import sys
import subprocess
import os
from datetime import datetime

def run_command(script_path, stage_name):
    """Run a Python script and handle its output"""
    print("\n" + "="*80)
    print(f"STAGE: {stage_name}")
    print("="*80)
    print(f"Running: {script_path}")
    print("-"*80 + "\n")
    
    try:
        # Run the script and capture output
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=False,  # Show output in real-time
            text=True,
            check=True
        )
        
        print("\n" + "-"*80)
        print(f"✓ {stage_name} completed successfully!")
        print("="*80)
        return True
        
    except subprocess.CalledProcessError as e:
        print("\n" + "-"*80)
        print(f"✗ {stage_name} failed!")
        print(f"Error code: {e.returncode}")
        print("="*80)
        return False

def main():
    """Run the complete ETL pipeline"""
    start_time = datetime.now()
    
    print("\n" + "="*80)
    print("=" + " "*15 + "AFRICA ENERGY DATA - FULL ETL PIPELINE" + " "*24 + "=")
    print("="*80)
    
    print(f"\nStarted at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Define pipeline stages
    stages = [
        {
            "name": "EXTRACT - Web Scraping",
            "script": os.path.join(project_root, "extract", "scraper_complete.py"),
            "description": "Scraping data from Africa Energy Portal"
        },
        {
            "name": "TRANSFORM - Step 1 (Wide Format)",
            "script": os.path.join(project_root, "transform", "transformer.py"),
            "description": "Transforming raw data to wide format"
        },
        {
            "name": "TRANSFORM - Step 2 (Long Format)",
            "script": os.path.join(project_root, "transform", "transform_to_long_format.py"),
            "description": "Converting wide format to MongoDB-ready long format"
        },
        {
            "name": "LOAD - MongoDB Upload",
            "script": os.path.join(project_root, "load", "load_to_mongodb.py"),
            "description": "Loading data into MongoDB Atlas"
        }
    ]
    
    # Run each stage
    print("\n" + "="*80)
    print("PIPELINE STAGES")
    print("="*80)
    for i, stage in enumerate(stages, 1):
        print(f"{i}. {stage['name']}")
        print(f"   {stage['description']}")
    
    print("\n" + "="*80)
    print("EXECUTION")
    print("="*80)
    
    for i, stage in enumerate(stages, 1):
        print(f"\n[{i}/{len(stages)}] {stage['name']}")
        
        # Check if script exists
        if not os.path.exists(stage['script']):
            print(f"✗ Error: Script not found: {stage['script']}")
            print(f"\nPipeline aborted at stage {i}")
            return False
        
        # Run the stage
        success = run_command(stage['script'], stage['name'])
        
        if not success:
            print(f"\n{'='*80}")
            print(f"{'PIPELINE FAILED':^80}")
            print(f"{'='*80}")
            print(f"\nFailed at stage {i}: {stage['name']}")
            print("Please check the error messages above and fix any issues.")
            return False
    
    # Success!
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n\n" + "="*80)
    print("PIPELINE COMPLETED SUCCESSFULLY!".center(80))
    print("="*80)
    
    print(f"\nStarted:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("✓ Data extracted from Africa Energy Portal")
    print("✓ Data transformed to long format")
    print("✓ Data loaded to MongoDB Atlas")
    print(f"\nDatabase: energyd2")
    print(f"Collection: test")
    print(f"\nYour data is ready to query!")
    print("\nExample MongoDB query:")
    print('  db.test.find({"country": "Nigeria", "year": 2018})')
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[!] Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[!] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
