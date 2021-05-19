import os
import sys
import yaml
from pprint import pprint

# NOTE
# - Each App uses a single app_config hardcoded to a specific directory
# - Each app has a single 'group' of steps used in the ETL job
# - A single job, dm-esp-extract, runs through the steps parallely
# - 

PARSE_PATH = '/Users/tnorlund/etl_aws_copy/apps'
# Get the different apps used by the current ETL pipeline
apps = os.listdir( PARSE_PATH )
apps.sort()
for app in apps:
    print( app )
sys.exit()

for app in apps:
    if os.path.exists( os.path.join( PARSE_PATH, app + '/config' ) ):
        app_config_files = [ file for file in os.listdir( os.path.join( PARSE_PATH, app + '/config' ) ) if file.endswith( '.yml') ]
    else:
        app_config_files = []
    
    if 'app_config.yml' in app_config_files:
        # parse and read the yaml 'app_config' file
        with open( os.path.join( PARSE_PATH, app + '/config/app_config.yml' ) ) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            app_config = yaml.load(file, Loader=yaml.FullLoader)
        # print( f'{app}: {app_config.keys()}'  )
        print( app )
        pprint( app_config['groups'][0]['steps'] )
        
        # Iterate over the different steps for the one group per app
        # if len( app_config['groups'] ) > 0 and 'steps' in app_config['groups'][0].keys():
        for step in app_config['groups'][0]['steps']:
            print( step )
        # else:
        #     print( f'{app} does not have steps' )
