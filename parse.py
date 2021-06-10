#!/usr/bin/env python3
import os
import json
import pandas as pd
import sqlparse
from sql_metadata import Parser


these_tables = [
    "dmt.ga_visit_summary",
    "stg.ga_src_prod",
    "dmt.ga_visit_level_getstartedcomplete_page_metadata",
    "dmt.d_customer_plan_360",
    "stg.ps_scheduled_carts",
    "stg.ps_scheduled_cart_items",
    "stg.mbo_order_base",
    "dmt.f_invoice",
    "dmt.f_invoice_product",
    "stg.ps_close_scheduled_cart_items",
    "stg.ps_change_scheduled_cart_item_events",
    "stg.ps_change_plan_item_events",
    "stg.ps_change_plan_attribute_events",
    "stg.ps_plan_items",
    "stg.ps_plans",
    "dmt.d_bundle_discount",
    "dmt.d_bundle_discounted_value"
]

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

# Read the current tables used by the SA team
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
# Read the current tables used by the ETL pipeline
app_df = pd.read_csv(
    'UsedApps.csv'
)
# Get the 'apps' that are found in the S3 pull and in the current Confluence documentation
PARSE_PATH = '/Users/tnorlund/etl_aws_copy/apps'
apps = list(
    set( os.listdir( PARSE_PATH ) )& set( app_df['App Name'].to_list() )
)

parsed_statements = 0
not_parsed_statements = 0
data = {}
table_data = {}
# Iterate over the different apps to find the different SQL queries per app
for app in apps:
    print( app )
    data[app] = {}
    table_data[app] = {}
    if os.path.exists( os.path.join( PARSE_PATH, app + '/sql' ) ):
        app_sql_files = os.listdir( os.path.join( PARSE_PATH, app + '/sql' ) )
    else:
        app_sql_files = []
    for sql_file in [ file for file in app_sql_files if file.endswith( '.sql' ) ]:
        data[app][sql_file] = []
        table_data[app][sql_file] = []
        # Read the SQL query contents in order to parse each statement
        sql_contents = open( 
            os.path.join( os.path.join( PARSE_PATH, app + '/sql' ), sql_file ) 
        ).read()
        for sql_statement in sqlparse.split( sql_contents ):
            parsed = sqlparse.parse( sql_statement )[0]
            sql_type = parsed.get_type()
            # Record the changed table when there is a SELECT, INSERT, CREATE, or DELETE
            if sql_type == 'SELECT' or sql_type == 'CREATE' \
            or sql_type == 'DELETE' or sql_type == 'INSERT':
                try:
                    metadata = Parser( parsed.value )
                    if sql_type == 'CREATE' or sql_type == 'INSERT' or sql_type == 'DELETE' and \
                    len([table_name for table_name in metadata.tables if table_name in these_tables]) > 0:
                        table_data[app][sql_file] = [table_name for table_name in metadata.tables if table_name in these_tables]
                    data[app][sql_file].append( {
                        'type':sql_type,
                        'columns':metadata.columns_dict,
                        'tables':metadata.tables,
                        'subqueries':metadata.subqueries,
                        'skipped': False,
                        'value': parsed.value
                    } )
                except:
                    not_parsed_statements += 1
                    data[app][sql_file].append( {
                        'skipped': True,
                        'value': parsed.value
                    } )

with open('tables_for_kyle.json', 'w') as json_file:
  json.dump(table_data, json_file)
tables = {}
for app in data.keys():
    tables[app] = []
    for sql_file in data[app].keys():
        for sql_statement in data[app][sql_file]:
            if 'tables' in sql_statement:
                for table in sql_statement['tables']:
                    if table not in tables[app]:
                        tables[app].append( table )

out = {}
# Tables given by @sujay.kar
# https://docs.google.com/spreadsheets/d/1N6PS2BmfQNAkKvIeKOPCmCZaVXszkSuhDJOZXLCKSeY/edit?usp=sharing
tables_in_gsheet = [table_name.lower() for table_name in table_df['Table Name'].to_list()]
# Iterate over the different Databrick Jobs
for index, row in app_df.iterrows():
    out[ row['App Name'] ] = {}
    out[ row['App Name'] ]['found'] = []
    out[ row['App Name'] ]['not found'] = []
    out[ row['App Name'] ]['unknown'] = []
    tables_used = tables[ row['App Name'] ]
    # Remove the data source from the list of tables found in this App
    tables_found =  [ 
        table for table in tables_used 
        if table_name_cleaner( table.lower() ) in tables_in_gsheet 
        # if table.lower() in table_df[['Data Source', 'Table Name']].agg('.'.join, axis=1).str.lower().to_list()
    ]
    tables_not_found = [ 
        table for table in tables_used 
        if not table_name_cleaner( table.lower() ) in tables_in_gsheet 
    ]
    # There are 3 possible outcomes of the table:
    # 1. The table and datasource was found
    # 2. The table was found with another datasource
    # 3. The table was not found
    for table in tables_found:
        found_in_gsheet = table in table_df[['Data Source', 'Table Name']].agg('.'.join, axis=1).str.lower().to_list()
        if found_in_gsheet:
            out[ row['App Name'] ]['found'].append( table )
            print( f'\t[X] {table}' )
        if table_name_cleaner( table.lower() ) not in [ table_name_cleaner( table.lower() ) for table in tables_found ]:
            out[ row['App Name'] ]['not found'].append( table )
            print( f'\t[ ] {table}' )
    for table in tables_not_found:
        print(f'\t[?] {table}')
        out[ row['App Name'] ]['unknown'].append( table )

# out_df = pd.DataFrame( columns=['Source', 'Name', 'Found In Documentation'] )
out_dict = {
    'Source':[], 'Name':[], 'Found In Documentation':[]
}
for app in out.keys():
    found_tables = out[app]
    sources = [table.split('.')[0] for table in found_tables]

with open('known_tables.json', 'w') as json_file:
  json.dump(out, json_file)