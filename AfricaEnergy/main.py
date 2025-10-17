import os
from pathlib import Path

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient

from extract.scrape import scrape_all_sectors
from load.load_economic import load_social_data
from load.load_electrical import load_electrical_data
from load.load_energy import load_energy_data


def run_loaders(collection):
    """Load staged CSV data into MongoDB."""
    load_energy_data(collection)
    load_electrical_data(collection)
    load_social_data(collection)


def main():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI is not set; please configure the connection string in the environment.")

    client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
    database = client.get_database("energyd2")
    collection = database.get_collection("test3")

    project_root = Path(__file__).resolve().parent
    staging_dir = project_root / "staging_data"
    headless_env = os.getenv("SCRAPER_HEADLESS", "").strip().lower()
    headless = headless_env in {"1", "true", "yes", "on"}

    if headless:
        print("Running scraper in headless mode.")
    else:
        print("Running scraper with visible browser window. Set SCRAPER_HEADLESS=true to override.")

    print("Starting extraction...")
    scrape_all_sectors(output_dir=staging_dir, headless=headless)

    print("Starting load phase...")
    run_loaders(collection)

    print("[OK] ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()
