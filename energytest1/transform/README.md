# Transform Module

This module handles data transformation to match the required schema.

## Required Schema

```
["country", "country_serial", "metric", "unit", "sector", "sub_sector", 
 "sub_sub_sector", "source_link", "source", "2000", "2001", ..., "2024"]
```

## Transformation Steps

1. Parse raw data from extraction
2. Pivot data to wide format (years as columns)
3. Add metadata columns (source_link, source, etc.)
4. Validate data types and completeness
5. Export to intermediate format for loading

## Usage

```python
from transformer import EnergyDataTransformer

transformer = EnergyDataTransformer()
transformed_data = transformer.transform(raw_data)
```
