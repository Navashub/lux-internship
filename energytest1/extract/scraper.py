"""
Africa Energy Portal Data Scraper
Extracts energy data from https://africa-energy-portal.org/
Saves to CSV file
"""

import time
import json
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd


class AfricaEnergyPortalScraper:
    def __init__(self, headless=True):
        self.base_url = "https://africa-energy-portal.org"
        self.database_url = f"{self.base_url}/database"
        self.headless = headless
        self.driver = None
        
    def setup_driver(self):
        """Initialize Selenium WebDriver with Chrome"""
        chrome_options = Options()
        # Disabled headless to bypass Cloudflare
        # if self.headless:
        #     chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        return self.driver
    
    def close_driver(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            
    def get_country_list(self):
        """Extract list of all African countries from the portal"""
        countries = [
            "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
            "Cameroon", "Cape Verde", "Central African Republic", "Chad", "Comoros",
            "Congo Democratic Republic", "Congo Republic", "Cote d'Ivoire", "Djibouti",
            "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon",
            "Gambia", "Ghana", "Guinea", "Guinea Bissau", "Kenya", "Lesotho", "Liberia",
            "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius", "Morocco",
            "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe",
            "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan",
            "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe"
        ]
        return countries
    
    def extract_database_data(self, year="2022", region="All", country="All"):
        """
        Extract data from the database page with filters
        
        Args:
            year: Year to filter (2000-2022)
            region: Region filter
            country: Country filter
        """
        print(f"Extracting data for Year: {year}, Region: {region}, Country: {country}")
        
        try:
            self.driver.get(self.database_url)
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, 20)
            time.sleep(5)  # Additional wait for dynamic content
            
            # Try to click the year filter
            print("Attempting to set year filter...")
            try:
                year_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Select Year')]"))
                )
                year_button.click()
                time.sleep(1)
                
                # Select specific year
                year_option = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{year}')]")
                year_option.click()
                time.sleep(3)
            except Exception as e:
                print(f"Could not set year filter: {e}")
            
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Try to find the data table
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables on the page")
            
            if tables:
                # Parse the main data table
                for idx, table in enumerate(tables):
                    print(f"\nTable {idx + 1}:")
                    df = pd.read_html(str(table))[0]
                    print(df.head())
                    
                return tables
            else:
                print("No tables found. The data might be loaded via API.")
                return None
                
        except Exception as e:
            print(f"Error extracting data: {e}")
            return None
    
    def test_single_country(self, country_name="nigeria"):
        """
        Test extraction from a single country profile page
        
        Args:
            country_name: Name of the country (lowercase, hyphenated)
        """
        country_url = f"{self.base_url}/aep/country/{country_name}"
        print(f"Testing country page: {country_url}")
        
        try:
            self.driver.get(country_url)
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, 20)
            time.sleep(5)
            
            # Get page source
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract basic country info
            print(f"\nCountry: {country_name.title()}")
            
            # Look for data tables
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables")
            
            # Try to find charts/data indicators
            # The portal uses Highcharts, data might be in JavaScript
            scripts = soup.find_all('script')
            print(f"Found {len(scripts)} script tags")
            
            return {
                'country': country_name,
                'tables_found': len(tables),
                'scripts_found': len(scripts)
            }
            
        except Exception as e:
            print(f"Error extracting country data: {e}")
            return None


    def extract_all_data_to_csv(self, output_file="africa_energy_data.csv"):
        """
        Extract all available data from the database and save to CSV
        """
        print(f"\n{'='*70}")
        print("EXTRACTING ALL DATA FROM AFRICA ENERGY PORTAL")
        print(f"{'='*70}\n")
        
        all_data = []
        
        try:
            # Navigate to database page
            print("[1/3] Loading database page...")
            self.driver.get(self.database_url)
            print("      Waiting for Cloudflare and page load (15 seconds)...")
            time.sleep(15)
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Find the main data table
            print("\n[2/3] Extracting data table...")
            tables = soup.find_all('table')
            
            if not tables:
                print("      ERROR: No data table found!")
                return False
            
            print(f"      Found {len(tables)} tables on the page")
            
            # The main data table should contain country data
            for idx, table in enumerate(tables):
                try:
                    # Try to parse with pandas
                    df = pd.read_html(str(table))[0]
                    
                    # Check if this looks like the main data table
                    if 'Country' in df.columns or 'country' in str(df.columns).lower():
                        print(f"\n      Table {idx + 1}: Main data table found!")
                        print(f"      Shape: {df.shape[0]} rows x {df.shape[1]} columns")
                        print(f"      Columns: {list(df.columns)[:5]}...")
                        
                        all_data.append(df)
                    else:
                        print(f"      Table {idx + 1}: {df.shape} (skipped - not main data)")
                        
                except Exception as e:
                    print(f"      Table {idx + 1}: Could not parse - {e}")
                    continue
            
            if not all_data:
                print("\n      WARNING: No parseable data tables found.")
                print("      The data might be loaded dynamically. Trying alternative method...")
                
                # Try to get the rendered table data
                try:
                    # Look for the data table by class or id
                    data_table = self.driver.find_element(By.CSS_SELECTOR, "table")
                    html = data_table.get_attribute('outerHTML')
                    df = pd.read_html(html)[0]
                    all_data.append(df)
                    print(f"      Successfully extracted: {df.shape[0]} rows x {df.shape[1]} columns")
                except Exception as e:
                    print(f"      Alternative method failed: {e}")
                    return False
            
            # Combine all data
            print(f"\n[3/3] Saving data to CSV...")
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Create output directory if it doesn't exist
                output_dir = os.path.dirname(output_file)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                
                # Save to CSV
                combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                
                print(f"      [OK] Data saved successfully!")
                print(f"      File: {output_file}")
                print(f"      Total rows: {len(combined_df)}")
                print(f"      Total columns: {len(combined_df.columns)}")
                print(f"\n      Preview of data:")
                print(combined_df.head())
                
                return True
            else:
                print("      ERROR: No data extracted!")
                return False
                
        except Exception as e:
            print(f"\n      ERROR during extraction: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main scraper execution"""
    print("\n" + "="*70)
    print("AFRICA ENERGY PORTAL DATA SCRAPER")
    print("="*70)
    
    # Set output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"africa_energy_data_{timestamp}.csv"
    
    scraper = AfricaEnergyPortalScraper(headless=False)
    
    try:
        print("\n[SETUP] Initializing Chrome driver...")
        scraper.setup_driver()
        print("        [OK] Driver ready!")
        
        # Extract all data
        success = scraper.extract_all_data_to_csv(output_file)
        
        if success:
            print(f"\n{'='*70}")
            print("EXTRACTION COMPLETED SUCCESSFULLY!")
            print(f"{'='*70}")
            print(f"\nOutput file: {output_file}")
            print("\nYou can now proceed with data transformation.")
        else:
            print(f"\n{'='*70}")
            print("EXTRACTION FAILED")
            print(f"{'='*70}")
            print("\nPlease check the error messages above.")
        
    except Exception as e:
        print(f"\n[ERROR] Fatal error during scraping: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n[CLEANUP] Closing browser...")
        scraper.close_driver()
        print("           [OK] Done!")


if __name__ == "__main__":
    main()
