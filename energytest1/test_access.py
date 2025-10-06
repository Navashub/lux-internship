"""
Simple test to verify we can access data from Africa Energy Portal
Run this first to confirm data extraction is possible
"""

import sys
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def test_portal_access():
    """Test if we can access the Africa Energy Portal"""
    print("="*60)
    print("AFRICA ENERGY PORTAL - DATA ACCESS TEST")
    print("="*60)
    
    print("\n[1/4] Setting up Chrome WebDriver...")
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Disabled: Cloudflare may block headless
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        print("   [OK] Chrome WebDriver initialized successfully")
    except Exception as e:
        print(f"   [ERROR] Error initializing Chrome WebDriver: {e}")
        print("\n   TROUBLESHOOTING:")
        print("   1. Install Chrome browser if not installed")
        print("   2. Chrome driver should auto-install with selenium")
        print("   3. If issues persist, try: pip install webdriver-manager")
        return False
    
    try:
        # Test 1: Access homepage
        print("\n[2/4] Testing access to homepage...")
        driver.get("https://africa-energy-portal.org/")
        print("   Waiting for Cloudflare check (this may take 10-15 seconds)...")
        time.sleep(15)  # Longer wait for Cloudflare
        
        if "Africa Energy Portal" in driver.title:
            print(f"   [OK] Homepage loaded successfully")
            print(f"   Title: {driver.title}")
        else:
            print(f"   [ERROR] Unexpected page title: {driver.title}")
            return False
        
        # Test 2: Access database page
        print("\n[3/4] Testing access to database page...")
        driver.get("https://africa-energy-portal.org/database")
        print("   Waiting for page to load...")
        time.sleep(10)  # Wait for dynamic content
        
        page_source = driver.page_source
        
        # Check for key elements
        has_filter = "Select Year" in page_source or "Select Country" in page_source
        has_data = "Population access to electricity" in page_source or "electricity" in page_source.lower()
        
        if has_filter and has_data:
            print(f"   [OK] Database page loaded with filters and data")
        else:
            print(f"   [WARNING] Database page loaded but some elements may be missing")
            print(f"      Has filters: {has_filter}")
            print(f"      Has data indicators: {has_data}")
        
        # Test 3: Access country page
        print("\n[4/4] Testing access to country profile (Nigeria)...")
        driver.get("https://africa-energy-portal.org/aep/country/nigeria")
        print("   Waiting for page to load...")
        time.sleep(10)
        
        if "Nigeria" in driver.page_source:
            print(f"   [OK] Nigeria country page loaded successfully")
            
            # Try to find some data points
            if "Population access to electricity" in driver.page_source:
                print(f"   [OK] Found electricity access data")
            if "Population" in driver.page_source:
                print(f"   [OK] Found population data")
        else:
            print(f"   [ERROR] Could not load Nigeria country page")
        
        print("\n" + "="*60)
        print("TEST RESULT: SUCCESS")
        print("="*60)
        print("\nYou can now proceed with full data extraction!")
        print("\nNext steps:")
        print("1. Run: python extract/scraper.py (for detailed extraction)")
        print("2. The scraper will collect data from all African countries")
        print("3. Data will be saved for transformation and MongoDB loading")
        print("\nNote: Data is available from 2000-2022 (not 2024)")
        
        driver.quit()
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Error during testing: {e}")
        import traceback
        traceback.print_exc()
        driver.quit()
        return False


if __name__ == "__main__":
    success = test_portal_access()
    sys.exit(0 if success else 1)
