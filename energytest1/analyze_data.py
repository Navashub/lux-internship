import pandas as pd

df = pd.read_csv('africa_energy_complete_20251006_202004.csv')

print('='*80)
print('DATA ANALYSIS REPORT')
print('='*80)

print(f'\nTotal rows: {len(df)}')
print(f'Total columns: {len(df.columns)}')

print(f'\nColumns: {list(df.columns)}')

if 'Country_Name' in df.columns:
    countries = sorted(df['Country_Name'].unique())
    print(f'\nCountries extracted ({len(countries)}):')
    for i in range(0, len(countries), 5):
        print('  ', ', '.join(countries[i:i+5]))

print('\nColumn analysis:')
for col in df.columns:
    print(f'  {col}: {df[col].nunique()} unique values, {df[col].isna().sum()} nulls')

print('\nSample data (first 5 rows):')
print(df.head())

print('\nSample data (last 5 rows):')
print(df.tail())
