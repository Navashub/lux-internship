# Extract Module

This module handles data extraction from the Africa Energy Portal.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Install Chrome WebDriver:
   - Download ChromeDriver from https://chromedriver.chromium.org/
   - Ensure it matches your Chrome browser version
   - Add to PATH or place in project directory

## Usage

Run the test scraper:
```bash
python scraper.py
```

## Data Source

- Portal: https://africa-energy-portal.org/
- Database: https://africa-energy-portal.org/database
- Country Profiles: https://africa-energy-portal.org/aep/country/{country-name}

## Notes

- Data is available from 2000-2022 (not 2024)
- Portal uses JavaScript for dynamic content loading
- Selenium is required for data extraction
