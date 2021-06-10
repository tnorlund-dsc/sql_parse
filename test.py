#!/usr/bin/env python3
# import os
# import json
# import pandas as pd
# import sqlparse
# from sql_metadata import Parser

# file_path = "/Users/tnorlund/etl_aws_copy/apps/dm-transform/sql/transform.dmt.f_invoice.sql"

# # Read the SQL query contents in order to parse each statement
# sql_contents = open(file_path).read()
# for sql_statement in sqlparse.split( sql_contents ):
#     parsed = sqlparse.parse( sql_statement )[0]
#     sql_type = parsed.get_type()
#     # # Record the changed table when there is a SELECT, INSERT, CREATE, or DELETE
#     # if sql_type == 'SELECT' or sql_type == 'CREATE' \
#     # or sql_type == 'DELETE' or sql_type == 'INSERT':
#     try:
#         metadata = Parser( parsed.value )
#         print(metadata.subqueries)
#     except Exception as e:
#         print(e)


import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
 
 
# Supported join method
ALL_JOIN_TYPE = (
    'LEFT JOIN', 
    'RIGHT JOIN', 
    'INNER JOIN', 
    'FULL JOIN', 
    'LEFT OUTER JOIN', 
    'FULL OUTER JOIN'
)
 
 
def is_subselect(parsed):
    """
         Subquery
    :param parsed:
    :return:
    """
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False
 
 
def extract_from_part(parsed):
    """
         Module after extracting from
    :param parsed:
    :return:
    """
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                from_seen = False
                continue
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
 
 
def extract_join_part(parsed):
    """
         After submitting the join module
    :param parsed:
    :return:
    """
    flag = False
    for item in parsed.tokens:
        if flag:
            if item.ttype is Keyword:
                flag = False
                continue
            else:
                yield item
        if item.ttype is Keyword and item.value.upper() in ALL_JOIN_TYPE:
            flag = True
 
 
def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        elif item.ttype is Keyword:
            yield item.value
 
 
def extract_tables(sql):
    """
         Extract the table name in sql (select statement)
    :param sql:
    :return:
    """
    from_stream = extract_from_part(sqlparse.parse(sql)[0])
    join_stream = extract_join_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(from_stream)) + list(extract_table_identifiers(join_stream))
 
 
 
print(extract_tables("""INSERT INTO dmt.f_invoice
select
    i.id as invoice_id
  , i.order_id
  , i.customer_id
  , s.id as shipment_id
  , i.sub_total
  , i.total_shipping
  , i.profit
  , i.profit_margin
  , i.total_line_item_shipping_discount
  , i.total_discount
  , i.base_shipping_discount
  , i.base_shipping
  , i.status as invoice_status -- only paid/ unpaid
  , i.total_tax --
  , i.total_line_item_shipping_tax
  , i.total_line_item_shipping
  , i.base_discount
  , i.base_tax
  , i.total_shipping_discount
  , i.grand_total
  , (ISNULL(i.grand_total,0) - ISNULL(i.total_tax,0)) as amount_true
  , i.total_line_item_discount
  , i.total_line_item_tax
  , i.created_at invoice_created_at
  , i.base_shipping_tax
  , i.total_cost
  , i.total_shipping_tax
  , i.balance
  , ocs.first_order_date as first_order_date
  , DATE_TRUNC('month',ocs.first_order_date)::date as first_order_month
  , scs.first_subscription_start_date as first_subscription_date
  , DATE_TRUNC('month',scs.first_subscription_start_date)::date as first_subscription_month
  , case when ocs.first_order_id = i.order_id then TRUE else FALSE end as initial_invoice
  , case when ocs.first_order_id = i.order_id then 'Initial'  else 'Recurring' end as invoice_revenue_type
  , case when m_ocr.reason_id =14  -- NSRQ
      then m_ocr.reason_name
      else
        case when ocs.first_order_id = i.order_id then 'Initial'  else 'Recurring' end
    end as invoice_revenue_type_name
  ---- customer
  , i.customer_created_at
  -- , test_customer all test customers are excluded
  , i.customer_type
  , left(i.customer_first_name,64) as customer_first_name
  , left(i.customer_last_name,64) as customer_last_name
  , i.customer_gender
  , left(i.customer_email, 128) as customer_email
  ----- orders
  , o.created_at as order_created_at
  , o.updated_at order_updated_at
  , o.created_by as order_created_by
  , i.order_base_shipping_charges
  , i.order_creation_reason
  , o.creation_reason as order_creation_reason_id
  , case when m_ocr.reason_id is null then 'Other' else m_ocr.reason_name end as order_creation_reason_name
  , case when m_or.reason_id is null then 'Other' else m_or.reason_name end as order_reason_name
  , case when o.creation_reason = 14 then 'true' else 'false' end nsrq
  , case when o.creation_reason = 14 and (o.created_at <  DATE_TRUNC('day', scs.first_subscription_start_date) OR scs.first_subscription_start_date is null) THEN 'true' else 'false' end as nsrq_guest_purchase
  , case when datediff(second, scs.curr_subscription_start_date, o.created_at) between 0 and 60 then 'true' else 'false' end as funnel_purchase_subscription
  , case when datediff(second, scs.first_subscription_start_date, o.created_at) between 0 and 60 then 'true' else 'false' end as initial_purchase_customer
  , i.order_creation_source
  , i.order_currency_code
  , i.order_discount
  , i.order_ignore_shipping_charges
  , i.order_is_on_the_house
  , i.order_shipping_charges
  , i.shop_id
  , case when m_shop.shop_id is null then 'Other' else m_shop.shop_name end as shop_name
  , i.order_sub_total
  , i.order_tax
  , i.order_total
  , i.order_total_shipping_charges
  -----// orders
  , i.tax_rate_id
  , i.tax_rate_country
  , i.tax_rate_state_province
  , i.tax_rate_county
  , i.tax_rate_post_code
  , i.tax_rate
  , i.shipping_address_id
  , i.shipping_address_country
  , i.shipping_address_state_province
  , i.shipping_address_post_code
  , o.customer_subscription_id
  , o.coupon_instance_id
  , o.confirmation_email_instance_id
  , o.customer_subscription_bill_run_id
  , case when s.id is null then FALSE else TRUE end as shipment_created
  ---- shipment
  , s.tracking_number as shipment_tracking_number
  , s.price as shipment_price
  , s.tax as shipment_tax
  , s.created_at as shipment_created_at
  , s.updated_at as shipment_updated_at
  , left(s.status,32) as shipment_status_code
  , case when m_ss.status_code is null then 'Other' else m_ss.status_name end as shipment_status_name
  , case when m_ss.status_code is null then 'Other' else m_ss.push_status end as shipment_push_status
  , left(s.provider_status, 64) as shipment_provider_status
  , left(s.provider_method, 64) as shipment_provider_method
  , s.total_weight as shipment_total_weight
  , CASE
      WHEN s.total_weight  <= 9 THEN '00' + ceiling(s.total_weight)::varchar(16) + ' oz'
      WHEN s.total_weight  <= 15 THEN '0' + ceiling(s.total_weight)::varchar(16) + ' oz'
      WHEN s.total_weight  <= 16 THEN '1 lb'
      WHEN s.total_weight  > 15 THEN ceiling(s.total_weight / 16.0)::varchar(16) + ' lbs'
    END  as shipment_weight_tier
  , s.packaging_weight as shipment_packaging_weight
  , s.contents_weight as shipment_contents_weight
  , s.delivered_at as shipment_delivered_at
  , s.fulfillment_provider_id as shipment_fulfillment_provider_id
  , left(s.fulfillment_provider_name,64) as shipment_fulfillment_provider_name
  , case when m_warehouse.warehouse_id is null then 'Other' else m_warehouse.warehouse_name end as warehouse_name
  , s.inventory_location_id as shipment_inventory_location_id
  , s.order_fulfillment_id as shipment_order_fulfillment_id
  , s.shipped_at as shipment_shipped_at
  , s.shipment_cost as shipment_cost
  , s.handling_cost as shipment_handling_cost
  , case when m_or.reason_id is null then 'Regular' else m_or.reason_name end as shipment_type
  , i.cost_calculation_completed
  , i.billing_address_id
  , i.billing_address_country
  , i.billing_address_state_province
  , i.billing_address_post_code
  , getdate() as dw_load_date_time
-- bm2
  , i.order_attribution_id
  , i.order_attribution_attribution_id
  , i.order_attribution_attribution_type
  , i.order_attribution_created_at
  , i.order_attribution_updated_at
  , case when i.order_id = nvl(cip.order_id,000000000) then TRUE else FALSE end as plan_initial_invoice
  , case when s.uuid is null then s.id::varchar else s.uuid end as shipment_uuid	
  , case when s.primary_shipment_uuid is null and s.uuid is null and s.shipped_at is not null then s.id::varchar else s.primary_shipment_uuid end as primary_shipment_uuid 	
from dm_delta dd
  inner join stg.erp_invoices as i
    on dd.customer_id = i.customer_id
  -- invoice cannot exist without order - so inner join
  inner join stg.orders o
    on i.order_id = o.id
  left outer join stg.erp_shipments s
    on i.order_id = s.order_id
  left outer join map.shop m_shop
    on i.shop_id = m_shop.shop_id
  left outer join map.warehouse m_warehouse
    on s.fulfillment_provider_id = m_warehouse.warehouse_id
  left outer join map.order_creation_reason m_ocr
    on o.creation_reason = m_ocr.reason_id
  left outer join map.order_reason m_or
    on o.creation_reason = m_or.reason_id
  left outer join map.shipment_status m_ss
    on s.status = m_ss.status_code
  -- used for initial_invoice calc;
  -- TODO: swith to inner join when truncate replaced with delete in customer_status
  left outer join stg.customer_status cs
    on i.customer_id = cs.customer_id
  inner join (
        select
            o1.customer_id
          , o1.first_order_id
          , o1.first_order_date
        from (
          SELECT
              o.customer_id
            , FIRST_VALUE(o.id) OVER (PARTITION BY o.customer_id ORDER BY o.created_at rows between unbounded preceding and CURRENT ROW) as first_order_id
            --, LAST_VALUE(o.id) OVER (PARTITION BY o.customer_id ORDER BY o.created_at rows between unbounded preceding and CURRENT ROW) as last_order_id
            , FIRST_VALUE(o.created_at) OVER (PARTITION BY o.customer_id ORDER BY o.created_at rows between unbounded preceding and CURRENT ROW) as first_order_date
            --, LAST_VALUE(o.created_at) OVER (PARTITION BY o.customer_id ORDER BY o.created_at rows between unbounded preceding and CURRENT ROW)  as last_order_date
            , row_number() OVER (PARTITION BY o.customer_id ORDER BY o.created_at desc) as rnk
          FROM stg.orders o
            inner join dm_delta ddd
              on ddd.customer_id = o.customer_id
        )  o1
        where 1=1
          and o1.rnk =1
  ) ocs
    on dd.customer_id = ocs.customer_id
  left outer join (
        select
            s1.customer_id
          , s1.first_subscription_start_date
          , s1.curr_subscription_start_date
        from (
          SELECT
              s.customer_id
            , FIRST_VALUE(s.start_date_time) OVER (PARTITION BY s.customer_id ORDER BY s.start_date_time rows between unbounded preceding and CURRENT ROW) as first_subscription_start_date
            , LAST_VALUE(s.start_date_time) OVER (PARTITION BY s.customer_id ORDER BY s.start_date_time rows between unbounded preceding and CURRENT ROW)  as curr_subscription_start_date
            , row_number() OVER (PARTITION BY s.customer_id ORDER BY s.start_date_time desc) as rnk
          FROM stg.customer_subscriptions s
            inner join dm_delta ddd
              on ddd.customer_id = s.customer_id
          --where s.customer_id = 5737548
        )  s1
        where 1=1
          and s1.rnk =1
  ) scs
    on dd.customer_id = scs.customer_id
  left outer join (
      select
        customer_id
      from stg.test_customers
      group by customer_id
  ) tc
  ON dd.customer_id = tc.customer_id
  left outer join stg.customer_initial_plan cip
    on i.customer_id = cip.customer_id
where 1=1
  and tc.customer_id is null
;"""))
print(extract_tables("select x1, x2 from (select x1, x2 from (select x1, x2 from a)) left join b on a.id=b.id"))