import os
import pandas as pd

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
