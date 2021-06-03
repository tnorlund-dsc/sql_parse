import json
import pandas as pd

with open('known_tables.json') as f:
  data = json.load(f)

# out_df = pd.DataFrame( columns=['Source', 'Name', 'Found In Documentation'] )
out_dict = {
    'Source':[], 'Name':[], 'Found In Documentation':[]
}
for app in data.keys():
    found_tables = data[app]['found'] + data[app]['unknown']
    sources = [table.split('.')[0] for table in found_tables]
    name = ['.'.join(table.split('.')[1:]) for table in found_tables]
    found = [table in data[app]['found'] for table in found_tables]
    print( 
        pd.DataFrame( {
            'Source': [table.split('.')[0] for table in found_tables], 
            'Name': ['.'.join(table.split('.')[1:]) for table in found_tables], 
            'Found In Documentation': [table in data[app]['found'] for table in found_tables]
        } ).to_csv( f'{app}.csv' )
    )
