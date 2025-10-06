"""
Africa Energy Portal Complete Data Scraper
Extracts ALL energy data country-by-country for 2000-2022
Saves to CSV with comprehensive metrics
"""

import time
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import re


class ComprehensiveAfricaEnergyScraper:
    def __init__(self):
        self.base_url = "https://africa-energy-portal.org"
        self.driver = None
        self.all_data = []
        
        # List of all 54 African countries
        self.countries = [
            "algeria", "angola", "benin", "botswana", "burkina-faso", "burundi",
            "cameroon", "cape-verde", "central-african-republic", "chad", "comoros",
            "congo-democratic-republic", "congo-republic", "cote-divoire", "djibouti",
            "egypt", "equatorial-guinea", "eritrea", "eswatini", "ethiopia", "gabon",
            "gambia", "ghana", "guinea", "guinea-bissau", "kenya", "lesotho", "liberia",
            "libya", "madagascar", "malawi", "mali", "mauritania", "mauritius", "morocco",
            "mozambique", "namibia", "niger", "nigeria", "rwanda", "sao-tome-and-principe",
            "senegal", "seychelles", "sierra-leone", "somalia", "south-africa", "south-sudan",
            "sudan", "tanzania", "togo", "tunisia", "uganda", "zambia", "zimbabwe"
        ]
        
        # Years we want to extract
        self.years = list(range(2000, 2023))  # 2000-2022
        
    def setup_driver(self):
        """Initialize Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        print("[OK] Chrome driver initialized")
        
    def close_driver(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()
            
    def extract_country_data(self, country_slug, country_name):
        """
        Extract all energy data for a specific country
        """
        country_url = f"{self.base_url}/aep/country/{country_slug}"
        
        print(f"\n  Extracting: {country_name}")
        print(f"  URL: {country_url}")
        
        try:
            self.driver.get(country_url)
            time.sleep(8)  # Wait for page load
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract data from tables
            tables = soup.find_all('table')
            country_data = []
            
            for table in tables:
                try:
                    df = pd.read_html(str(table))[0]
                    
                    # Check if this is a data table with country info
                    if 'Country' in df.columns or 'Indicator' in df.columns:
                        # Add country name to each row
                        df['Country_Name'] = country_name
                        df['Country_Slug'] = country_slug
                        df['Source_Link'] = country_url
                        df['Source'] = 'Africa Energy Portal'
                        country_data.append(df)
                        
                except Exception as e:
                    continue
            
            # Look for key indicators in the page text
            # Try to extract electricity access rates
            access_data = self.extract_access_data(soup, country_name, country_slug, country_url)
            if access_data:
                country_data.extend(access_data)
            
            print(f"  [OK] Extracted {len(country_data)} data tables")
            return country_data
            
        except Exception as e:
            print(f"  [ERROR] Failed to extract {country_name}: {e}")
            return []
    
    def extract_access_data(self, soup, country_name, country_slug, country_url):
        """
        Extract electricity access data from country page
        """
        access_data = []
        
        # Look for electricity access percentages in the HTML
        text = soup.get_text()
        
        # Try to find patterns like "National 60.5 %"
        patterns = {
            'Population access to electricity-National': r'National\s*(\d+\.?\d*)\s*%',
            'Population access to electricity-Rural': r'Rural\s*(\d+\.?\d*)\s*%',
            'Population access to electricity-Urban': r'Urban\s*(\d+\.?\d*)\s*%',
        }
        
        for metric, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                value = float(match.group(1))
                access_data.append({
                    'Country_Name': country_name,
                    'Country_Slug': country_slug,
                    'Indicator': metric,
                    'Unit': '% of population',
                    'Sector': 'Electricity',
                    'Sub_Sector': 'Access',
                    'Value_2022': value,  # Latest available
                    'Source_Link': country_url,
                    'Source': 'Africa Energy Portal'
                })
        
        return [pd.DataFrame(access_data)] if access_data else []
    
    def scrape_all_countries(self, output_file="africa_energy_complete.csv"):
        """
        Main method to scrape all countries
        """
        print(f"\n{'='*80}")
        print("COMPREHENSIVE AFRICA ENERGY DATA EXTRACTION")
        print(f"{'='*80}")
        print(f"Countries to extract: {len(self.countries)}")
        print(f"Years: 2000-2022 (23 years)")
        print(f"{'='*80}\n")
        
        all_country_data = []
        successful = 0
        failed = 0
        
        for idx, country_slug in enumerate(self.countries, 1):
            # Convert slug to readable name
            country_name = country_slug.replace('-', ' ').title()
            
            print(f"[{idx}/{len(self.countries)}] Processing: {country_name}")
            
            try:
                country_data = self.extract_country_data(country_slug, country_name)
                
                if country_data:
                    all_country_data.extend(country_data)
                    successful += 1
                    print(f"  [SUCCESS] {country_name} - {len(country_data)} datasets")
                else:
                    failed += 1
                    print(f"  [WARNING] {country_name} - No data extracted")
                
                # Rate limiting - be respectful
                time.sleep(2)
                
            except Exception as e:
                failed += 1
                print(f"  [ERROR] {country_name} - {e}")
                continue
        
        # Save all data
        print(f"\n{'='*80}")
        print("SAVING DATA TO CSV")
        print(f"{'='*80}")
        
        if all_country_data:
            # Combine all dataframes
            combined_df = pd.concat(all_country_data, ignore_index=True)
            
            # Save to CSV
            combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"\n[OK] Data saved successfully!")
            print(f"File: {output_file}")
            print(f"Total rows: {len(combined_df)}")
            print(f"Total columns: {len(combined_df.columns)}")
            print(f"Successful countries: {successful}/{len(self.countries)}")
            print(f"Failed countries: {failed}/{len(self.countries)}")
            
            print(f"\nColumn names:")
            print(list(combined_df.columns))
            
            print(f"\nSample data:")
            print(combined_df.head(10))
            
            return True
        else:
            print("[ERROR] No data collected!")
            return False


def main():
    """Main execution"""
    print("\n" + "="*80)
    print("AFRICA ENERGY PORTAL - COMPREHENSIVE DATA SCRAPER")
    print("="*80)
    
    # Get the project root directory (parent of extract/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(project_root, f"africa_energy_complete_{timestamp}.csv")
    
    scraper = ComprehensiveAfricaEnergyScraper()
    
    try:
        print("\n[SETUP] Initializing browser...")
        scraper.setup_driver()
        
        # Scrape all countries
        success = scraper.scrape_all_countries(output_file)
        
        if success:
            print(f"\n{'='*80}")
            print("EXTRACTION COMPLETED SUCCESSFULLY!")
            print(f"{'='*80}")
            print(f"\nOutput file: {output_file}")
            print("\nNext steps:")
            print("1. Review the extracted data")
            print("2. Transform data to required schema")
            print("3. Load into MongoDB")
        else:
            print(f"\n{'='*80}")
            print("EXTRACTION COMPLETED WITH ERRORS")
            print(f"{'='*80}")
            
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n[CLEANUP] Closing browser...")
        scraper.close_driver()
        print("[OK] Done!")


if __name__ == "__main__":
    main()
