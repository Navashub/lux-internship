# Load Module

This module handles loading transformed data into MongoDB.

## MongoDB Schema

Each document represents one metric per country across all years.

```json
{
  "country": "Nigeria",
  "country_serial": 1,
  "metric": "Population access to electricity-National",
  "unit": "% of population",
  "sector": "Electricity",
  "sub_sector": "Access",
  "sub_sub_sector": null,
  "source_link": "https://africa-energy-portal.org/",
  "source": "Africa Energy Portal",
  "2000": 45.5,
  "2001": 46.0,
  ...
  "2024": null
}
```

## Setup

1. Install MongoDB locally or use MongoDB Atlas
2. Configure connection string in environment variables
3. Create database and collection

## Usage

```python
from loader import MongoDBLoader

loader = MongoDBLoader(connection_string="mongodb://localhost:27017/")
loader.load_data(transformed_data, collection_name="energy_data")
```
