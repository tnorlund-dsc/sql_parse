#!/usr/bin/env python3

import os
import sqlparse

PARSE_PATH = '/Users/tnorlund/etl_aws_copy/apps'
# Get the different apps used by the current ETL pipeline
apps = os.listdir( PARSE_PATH )
# Iterate over the different apps to find the different SQL queries per app
for app in apps:
    if os.path.exists( os.path.join( PARSE_PATH, app + '/sql' ) ):
        app_sql_files = os.listdir( os.path.join( PARSE_PATH, app + '/sql' ) )
    else:
        app_sql_files = []
    for sql_file in [ file for file in app_sql_files if file.endswith( '.sql' ) ]:
        # Read the SQL query contents in order to parse each statement
        sql_contents = open( os.path.join( os.path.join( PARSE_PATH, app + '/sql' ), sql_file ) ).read()
        for sql_statement in sqlparse.split( sql_contents ):
            parsed = sqlparse.parse( sql_statement )
            print( parsed )

        