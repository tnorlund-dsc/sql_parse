from pprint import pprint
import json
import itertools

with open('sql.json') as f:
  data = json.load(f)

tables = {}
for app in data.keys():
    tables[app] = []
    for sql_file in data[app].keys():
        for sql_statement in data[app][sql_file]:
            if 'tables' in sql_statement:
                for table in sql_statement['tables']:
                    if table not in tables[app]:
                        tables[app].append( table )
with open('apps_modifying_tables.json', 'w') as json_file:
  json.dump(tables, json_file)

# with open('apps_modifying_tables.json') as f:
#   tables = json.load(f)
out = {}
for app in tables.keys():
    temp_apps = list( tables.keys() )
    temp_apps.remove( app )
    all_other_tables = list( set( list( itertools.chain(*[ tables[temp_app] for temp_app in temp_apps] ) ) ) )
    out[app] = [ table for table in tables[ app ] if table in all_other_tables ]
with open('common_tables_between_apps.json', 'w') as json_file:
  json.dump(out, json_file)