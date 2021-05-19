import json
import pandas as pd
from pprint import pprint

def table_name_cleaner( table_name: str ) -> str:
    if ( table_name.startswith('dmt.') ):
        return table_name.replace('dmt.', '')
    if ( table_name.startswith('stg.') ):
        return table_name.replace('stg.', '')
    if ( table_name.startswith('map.') ):
        return table_name.replace('map.', '')
    if ( table_name.startswith('extract.') ):
        return table_name.replace('extract.', '')
    if ( table_name.startswith('tmp.') ):
        return table_name.replace('tmp.', '')
    if ( table_name.startswith('spectrum.') ):
        return table_name.replace('spectrum.', '')
    return table_name

# Read the Google Sheets export as a '.csv'
column_names = [
    'Table Name',
    'Focus Area',
    "Data Source",
    'Business Critical',
    'Business Purpose & Insights Driven'
    'Key Metrics',
    'Key Dimensions',
    "Joins"
]
table_df = pd.read_csv( 
    'KeyTables.csv', 
    names=column_names, 
    skiprows=[0, 1], 
    index_col=False 
)
table_df['Table Name'].str.strip()
table_df['Focus Area'].str.strip()
table_df["Data Source"].str.strip()
app_df = pd.read_csv(
    'UsedApps.csv'
)

# Read the '.json' of the apps modifying the different tables
with open('sql.json') as f:
    data = json.load(f)

with open( 'apps_modifying_tables.json' ) as f:
    tables = json.load( f )

# Tables given by @sujay.kar
# https://docs.google.com/spreadsheets/d/1N6PS2BmfQNAkKvIeKOPCmCZaVXszkSuhDJOZXLCKSeY/edit?usp=sharing
tables_in_gsheet = [table_name.lower() for table_name in table_df['Table Name'].to_list()]
# example = table_df[['Data Source', 'Table Name']].agg('.'.join, axis=1).str.lower().to_list()
# print( example )

# Iterate over the different Databrick Jobs
for index, row in app_df.iterrows():
    print( row['App Name'] )
    tables_used = tables[ row['App Name'] ]
    # Remove the data source from the list of tables found in this App
    tables_found =  [ 
        table for table in tables_used 
        if table_name_cleaner( table.lower() ) in tables_in_gsheet 
        # if table.lower() in table_df[['Data Source', 'Table Name']].agg('.'.join, axis=1).str.lower().to_list()
    ]
    for table in tables_found:
        found_in_gsheet = table in table_df[['Data Source', 'Table Name']].agg('.'.join, axis=1).str.lower().to_list()
        if found_in_gsheet:
            print( f'\t[X] {table}' )
        else:
            print( f'\t[ ] {table}' )
