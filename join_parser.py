#!/usr/bin/env python3

# Blatantly stolen from:
# https://www.programmersought.com/article/85254754672/
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison
from sqlparse.tokens import Keyword, DML

def is_subselect(token):
    """Returns whether the token has a ``SELECT`` statement in it"""
    if not token.is_group:
        return False
    for item in token.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(token):
    """Yields the ``FROM`` portion of a query"""
    from_seen = False
    # Iterate over the differnet tokens
    for _token in token.tokens:
        if from_seen:
            if is_subselect(_token):
                for __token in extract_from_part(_token):
                    yield __token
            elif _token.ttype is Keyword:
                from_seen = False
                continue
            else:
                yield _token
        if _token.ttype is Keyword and _token.value.upper() == 'FROM':
            from_seen = True

def extract_join_part(token):
    """Yields the ``JOIN`` portion of a query"""
    join_seen = False
    for _token in token.tokens:
        if join_seen:
            if _token.ttype is Keyword:
                join_seen = False
                continue
            else:
                yield _token
        if _token.ttype is Keyword and _token.value.upper() in (
            'LEFT JOIN', 
            'RIGHT JOIN', 
            'INNER JOIN', 
            'FULL JOIN', 
            'LEFT OUTER JOIN', 
            'FULL OUTER JOIN'
        ):
            join_seen = True

def extract_table_identifiers(token_stream):
    """Yields the unique identifiers found in the stream of tokens"""
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        elif item.ttype is Keyword:
            yield item.value

def print_to_console(token):
    for x in list(token):
        if x.is_whitespace:
            continue
        print('----')
        print(dir(x))
        print(x)

def extract_comparisons(token):
    for x in token.tokens:
        if isinstance(x, Comparison):
            yield(x.left.value, x.right.value)

def table_identifiers_to_dict(token_stream):
    """Creates a dictionary of the aliased names as keys and real names as values"""
    out = {}
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                out[identifier.get_name()] = identifier.get_real_name()
        elif isinstance(item, Identifier):
            # There is a chance of a subquery when the aliased name and the real name are the same.
            if item.get_name() == item.get_real_name():
                match = re.match(r'\(([\W\w]+)\)\s+' + item.get_name(),  item.value)
                # Use a regular expression to find the subquery and recursively call all the steps.
                if match:
                    print('+++++++++++++')
                    # print(match.groups()[0])
                    parsed = sqlparse.parse(match.groups()[0])[0]
                    print('comparisons', list(extract_comparisons(parsed)))
                    # print('comparisons length', len(list(extract_comparisons(parsed))))
                    # print('from length', len(list(extract_from_part(parsed))))
                    print('from', table_identifiers_to_dict(extract_from_part(parsed)))
                    print('join', table_identifiers_to_dict(extract_join_part(parsed)))
                else:
                    table_match = re.match(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', item.value)
                    if table_match:
                        print(table_match.groups())
                    else:
                        raise Exception('Could not find subquery')
            out[item.get_name()] = item.get_real_name()
        elif item.ttype is Keyword:
            out[item.get_name()] = item.get_real_name()
    return out


file_path = "/Users/tnorlund/etl_aws_copy/apps/dm-transform/sql/transform.dmt.f_invoice.sql"
sql_contents = open(file_path).read()
sql_contents = """CREATE TEMP TABLE dm_delta AS
select distinct i.customer_id as customer_id
from stg.erp_invoices i
  inner join stg.orders o
    on i.order_id = o.id
  left outer join stg.erp_shipments s
    on i.order_id = s.order_id
where (
         i.dsc_processed_at >= '<start_date>'::timestamp  -  interval '1 day'
         OR o.updated_at >= '<start_date>'::timestamp -  interval '1 day'
         OR s.dsc_processed_at >= '<start_date>'::timestamp -  interval '1 day'
       )
;"""


for sql_statement in sqlparse.split( sql_contents ):
    # Tokenize the SQL statement
    parsed = sqlparse.parse( sql_statement )[0]
    # print(list(extract_table_identifiers(extract_from_part(parsed))))
    # print(list(extract_table_identifiers(extract_join_part(parsed))))
    print(table_identifiers_to_dict(list(extract_join_part(parsed))))
    # for item in extract_join_part(parsed):
    #     if isinstance(item, Identifier):
    #         print('-----')
    #         print(item)
    #         print(type(item))
    #         print(item.get_name())
    #         print(item.get_real_name())

    # print_to_console(parsed)


    