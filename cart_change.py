import json
from pprint import pprint


tables = [
    'stg.ps_close_scheduled_cart_items',
    'stg.ps_change_scheduled_cart_item_events',
    'stg.ps_change_plan_item_events',
    'stg.ps_change_plan_attribute_events',
    'stg.ps_plan_items',
    'stg.ps_plans',
    'stg.ps_scheduled_carts',
    'stg.mbo_order_base'
]

with open('apps_modifying_tables.json') as f:
  data = json.load(f)

for table in tables:
    print( table )
    for app in data.keys():
        if table in data[app]:
            print( f'\t{app}' )

# for app in data.keys():
#     tables_app_uses = data[app]
