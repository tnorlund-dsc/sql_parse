import os
import yaml
import json
import pandas as pd
import sqlparse
from pprint import pprint
from sql_metadata import Parser
from utils import table_name_cleaner

# NOTE
# - Each App uses a single app_config hardcoded to a specific directory
# - Each app has a single 'group' of steps used in the ETL job
# - A single job, dm-esp-extract, runs through the steps parallely
# - 

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

# Find the '.sql' scripts each app uses
sql_scripts = {}
for app in apps:
    if os.path.exists( os.path.join( PARSE_PATH, app + '/config' ) ):
        sql_scripts[app] = []
        app_config_files = [ 
            file for file in os.listdir( os.path.join( PARSE_PATH, app + '/config' ) ) 
            if file.endswith( '.yml') 
        ]
    else:
        app_config_files = []
    
    if 'app_config.yml' in app_config_files:
        # parse and read the yaml 'app_config' file
        with open( os.path.join( PARSE_PATH, app + '/config/app_config.yml' ) ) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            app_config = yaml.load(file, Loader=yaml.FullLoader)
        
        scripts = [ 
            "tgt_load_sql"
            # 'tgt_load_sql_script', 
            # 'tgt_transform_sql_script', 
            # 'src_extract_sql_script', 
            # 'transform_sql_script' 
        ]
        # Iterate over the different steps for the one group per app
        for step in app_config['groups'][0]['steps']:
            # Each step uses certail '.sql' scripts to query against certain tables
            scripts_used_in_step = [ script for script in scripts if script in step.keys() ]
            for script in scripts_used_in_step:
                if os.path.exists( os.path.join( PARSE_PATH, app + '/sql/' + step[ script ] ) ):
                    sql_scripts[app].append(
                        os.path.join( PARSE_PATH, app + '/sql/' + step[ script ] )
                    )
print( sql_scripts )
# Store the data obtained from parsing each app's '.sql' script
data = {}
# Parse the SQL queries to determine which tables are used.
for app in apps:
    data[app] = {}
    for sql_file in sql_scripts[app]:
        data[app][sql_file] = []
        # Read the SQL query contents in order to parse each statement
        sql_contents = open( 
            os.path.join( os.path.join( PARSE_PATH, app + '/sql' ), sql_file ) 
        ).read()
        # Split the SQL query contents into the different queries made in the script
        for sql_statement in sqlparse.split( sql_contents ):
            parsed = sqlparse.parse( sql_statement )[0]
            sql_type = parsed.get_type()
            # Record the changed table when there is a SELECT, INSERT, CREATE, or DELETE
            if sql_type == 'SELECT' or sql_type == 'CREATE' \
            or sql_type == 'DELETE' or sql_type == 'INSERT':
                try:
                    metadata = Parser( parsed.value )
                    data[app][sql_file].append( {
                        'type':sql_type,
                        'columns':metadata.columns_dict,
                        'tables':metadata.tables,
                        'subqueries':metadata.subqueries,
                        'skipped': False,
                        'value': parsed.value
                    } )
                except:
                    data[app][sql_file].append( {
                        'skipped': True,
                        'value': parsed.value
                    } )
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
    print( row['App Name'] )
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
with open('known_tables.json', 'w') as json_file:
  json.dump(out, json_file)