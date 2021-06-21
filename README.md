# SQL Parsing

The goal of this repository is to understand how the different Redshift schemas and tables rely on one another. This will allow the data engineering team to prioritize and plan a transition to a more reliable system.

## parse.py
This script uses the known dm_etl_pkg 'apps' to determine the '.sql' scripts used per app.

## join_parse.py
This script uses a '.sql' file to determine its dependencies.