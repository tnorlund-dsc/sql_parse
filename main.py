#!/usr/bin/env python3
from re import S
import sys
import os
import json
import sqlparse
from pprint import pprint
from sql_metadata import Parser

PARSE_PATH = '/Users/tnorlund/etl_aws_copy/apps'
# Get the different apps used by the current ETL pipeline
apps = os.listdir( PARSE_PATH )

print( apps )

parsed_statements = 0
not_parsed_statements = 0
out = {}
# Iterate over the different apps to find the different SQL queries per app
for app in apps:
    out[app] = {}
    if os.path.exists( os.path.join( PARSE_PATH, app + '/sql' ) ):
        app_sql_files = os.listdir( os.path.join( PARSE_PATH, app + '/sql' ) )
    else:
        app_sql_files = []
    for sql_file in [ file for file in app_sql_files if file.endswith( '.sql' ) ]:
        out[app][sql_file] = []
        print( os.path.join( PARSE_PATH, app + '/sql/' + sql_file ) )
        # Read the SQL query contents in order to parse each statement
        sql_contents = open( os.path.join( os.path.join( PARSE_PATH, app + '/sql' ), sql_file ) ).read()
        for sql_statement in sqlparse.split( sql_contents ):
            parsed = sqlparse.parse( sql_statement )[0]
            sql_type = parsed.get_type()
            # Record the changed table when there is an INSERT, CREATE, or DELETE
            # print( parsed.get_type() )
            if sql_type == 'SELECT':
                try:
                    pprint( Parser( parsed.value ).columns_dict )
                except:
                    print('could not parse')
            if sql_type == 'CREATE' or sql_type == 'DELETE' or sql_type == 'INSERT':
                parsed_statements += 1
                try:
                    metadata = Parser( parsed.value )
                    out[app][sql_file].append( {
                        'type':sql_type,
                        'columns':metadata.columns_dict,
                        'tables':metadata.tables,
                        'subqueries':metadata.subqueries,
                        'skipped': False,
                        'value': parsed.value
                    } )
                    # pprint( metadata.columns_dict )
                    # pprint( metadata.subqueries )
                    # pprint( metadata.tables )
                except:
                    not_parsed_statements += 1
                    out[app][sql_file].append( {
                        'skipped': True,
                        'value': parsed.value
                    } )
                    print( 'couldn\'t parse' )
        # sys.exit()
print( f'parsed: {parsed_statements}\nnot parsed: {not_parsed_statements}' )
# with open('sql.json', 'w') as json_file:
#   json.dump(out, json_file)

        