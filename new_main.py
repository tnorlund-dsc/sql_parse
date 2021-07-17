"""Parses known ETL pipelines for dependencies.
"""
import os
import yaml
from dotenv import load_dotenv
from dependency import Dependency
import psycopg2

# Load the values found in the local ``.env`` file.
load_dotenv()

# The name of the directories the hold the configuration and '.sql' files used in the current ETL
# pipelines.
app_names = [
    'dm-extract',
    'dm-esp-extract',
    'dm-tmp-transform-prod',
    'dm-customer-status',
    'dm-dpr',
    'dm-optimove-export',
    'dm-transform',
    'dm-daily-reports',
    'dm-optimove-import',
    'dm-optouts',
    'dm-ga',
    'dm-ga360-export'
]

PARSE_PATH = os.getenv('PARSE_PATH')
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("REDSHIFT_USER")
PASSWORD = os.getenv("PASSWORD")

connection = psycopg2.connect(
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DATABASE,
)
cursor = connection.cursor()


for app in app_names[0:]:
    # Raise an exception when the app's directory cannot be found.
    if not os.path.exists(os.path.join(PARSE_PATH, app)):
        raise Exception(
            f"Could not find '{app}' in the directory: {os.path.join(PARSE_PATH, app)}"
        )
    # Raise an exception when the app's directory does not contain a ``./sql`` directory.
    if not os.path.exists(os.path.join(PARSE_PATH, app + '/sql')):
        raise Exception(
            f"Could not find './sql' directory in app directory: {os.path.join(PARSE_PATH, app)}"
        )
    # Raise an exception when the app's configuration directory does not have the
    # ``app_config.yml`` file in the ``./config`` directory.
    if not os.path.isfile(os.path.join(PARSE_PATH, app + '/config/app_config.yml')):
        raise Exception(
            "Could not find the 'app_config.yml' configuration file in the app directory: " \
                + os.path.join(PARSE_PATH, app)
        )
    # Get all the ``.sql`` files found in the ``./sql`` directory of the app.
    app_sql_files = [
        os.path.join(PARSE_PATH, app + f'/sql/{file}')
        for file in os.listdir(os.path.join(PARSE_PATH, app + '/sql'))
        if file.endswith('.sql')
    ]
    # Read the app's configuration ``.yml`` file.
    app_config = yaml.load(
        open(os.path.join(PARSE_PATH, app + '/config/app_config.yml')).read(),
        Loader=yaml.FullLoader
    )
    # Iterate over the different steps for the group per app.
    for step in app_config['groups'][0]['steps']:
        # Get the different ``.sql`` scripts used in the step.
        sql_files_in_step = [
            os.path.join(PARSE_PATH, app + f'/sql/{value}')
            for value in step.values() 
            if isinstance(value, str)
                and value.endswith('.sql') 
                and os.path.isfile(os.path.join(PARSE_PATH, app + f'/sql/{value}'))
        ]
        # Parse the dependencies for each ``.sql`` file found in the step.
        [Dependency(sql_file, cursor).parse() for sql_file in sql_files_in_step]
        # Dependency()
        # print(sql_files_in_step)
connection.commit()
connection.close()
