#!/usr/bin/env python3

# Blatantly stolen from:
# https://www.programmersought.com/article/85254754672/
import re
from pprint import pprint
from collections import ChainMap
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Token
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
        if _token.is_whitespace:
            continue
        if from_seen:
            if is_subselect(_token):
                for __token in extract_from_part(_token):
                    yield __token
            elif _token.ttype is Keyword:
                from_seen = False
                continue
            else:
                # The alias used to reference the table in the query
                alias = _token.get_name()
                # The full table name without the schema
                table_name = _token.get_real_name()
                # The Redshift schema where the table is accessed from
                schema = _token.value.replace(f".{table_name}", '').split(' ')[0]
                yield {
                    alias:{
                        'table_name': table_name,
                        'schema': schema,
                        'token': _token
                    }
                }
        if _token.ttype is Keyword and _token.value.upper() == 'FROM':
            from_seen = True

def extract_join_part(token):
    """Yields the ``JOIN`` portion of a query"""
    join_type = None
    for _token in token.tokens:
        # Ingore all whitespace tokens
        if _token.is_whitespace:
            continue
        if join_type:
            if _token.ttype is Keyword:
                join_type = None
                continue
            else:
                # The alias used to reference the table in the query
                alias = _token.get_name()
                # The full table name without the schema
                table_name = _token.get_real_name()
                # The Redshift schema where the table is accessed from
                redshift_schema = _token.value.replace(f".{table_name}", '').split(' ')[0]
                yield {
                    alias: {
                        'join_type':join_type, 
                        'table_name':table_name, 
                        'redshift_schema':redshift_schema,
                        'token': _token
                    }
                }
        if _token.ttype is Keyword and _token.value.upper() in (
            'LEFT JOIN', 
            'RIGHT JOIN', 
            'INNER JOIN', 
            'FULL JOIN', 
            'LEFT OUTER JOIN', 
            'FULL OUTER JOIN'
        ):
            join_type = _token.value.upper()

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
    if isinstance(parsed.tokens[0], Token) \
    and parsed.tokens[0].value.upper() == 'CREATE':
        # Get the name of the table being created
        table_name = next(token for token in parsed.tokens if isinstance(token, Identifier))
        # Get all the FROM statements's metadata
        froms = {k: v for d in extract_from_part(parsed) for k, v in d.items()}
        # Get all the JOIN statements's metadata
        joins = list(extract_join_part(parsed))
        # Get all of the comparisons to compare the number of comparisons to the number of JOIN statements
        comparisons = list(extract_comparisons(parsed))
        # When the number of comparisons does not match the number of joins, the parsing was incorrect, raise and exception.
        if len(comparisons) != len(joins):
            raise Exception('Parsing messed up!')
        out = {table_name:{'joins':[]}}
        # Set the join metadata 
        for index in range(len(joins)):
            join = joins[index]
            comparison_left = comparisons[index][0]
            comparison_right = comparisons[index][1]
            comparison_left_alias = comparison_left.split('.')[0]
            comparison_left_column = comparison_left.split('.')[1]
            comparison_right_alias = comparison_right.split('.')[0]
            comparison_right_column = comparison_right.split('.')[1]
            left = {
                **froms, 
                **{
                    k: v for d in joins for k, v in d.items()
                }
            }[comparison_left_alias]
            right = {
                **froms, 
                **{
                    k: v for d in joins for k, v in d.items()
                }
            }[comparison_right_alias]
            pprint(left)
            print(comparison_left_column)
            pprint(right)
            print(comparison_right_column)
            print()

            


    # print_to_console(parsed)


    