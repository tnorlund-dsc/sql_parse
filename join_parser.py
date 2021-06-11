#!/usr/bin/env python3

# Blatantly stolen from:
# https://www.programmersought.com/article/85254754672/
import re
from pprint import pprint
from collections import ChainMap
from numpy import select
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Token
from sqlparse.tokens import Keyword, DML
from sql_metadata import Parser

# TODO parse columns without using sql_metadata
def extract_selects(token):
    try:
        # print(f'token.value:\n{token.value}')
        match = re.findall(
            r'select([\s0-9a-zA-Z_\.,\\\/\(\)\':=<>+\-*]+)from', 
            token.value, 
            re.IGNORECASE|re.MULTILINE
        )
        if len(match) > 0:
            selections = match[0].split(',')
            for selection in selections:
                print(selection)
        else:
            print('could not match regular expression')
        metadata = Parser(token.value)
        select_columns = metadata.columns_dict['select']
        select_aliases = metadata.columns_aliases_dict['select']
        if len(select_aliases) != len(select_columns):
            raise Exception('Could not find column aliases')
        for index in range(len(select_columns)):
            yield {
                'schema': select_columns[index].split('.')[0],
                'table': select_columns[index].split('.')[1],
                'column': select_columns[index].split('.')[2],
                'alias': select_aliases[index]
            }
    except Exception as e:
        print(f'could not parse metadata:\n{e}')

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
                        'schema':redshift_schema,
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
        print(x)
        print(type(x))

def extract_comparisons(token):
    for x in token.tokens:
        if isinstance(x, Comparison):
            match = re.match(
                r'([a-zA-Z_]+)\.([a-zA-Z_]+)\s+=\s+([a-zA-Z_]+)\.([a-zA-Z_]+)', 
                x.value
            )
            if match:   
                yield (
                    match.groups()[0] + '.' + match.groups()[1], 
                    match.groups()[2] + '.' + match.groups()[3]
                )
            else:
                raise Exception(f'Could not find comparisons:\n{x.value}')

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


for sql_statement in sqlparse.split( sql_contents )[3:]:
    # Tokenize the SQL statement
    parsed = sqlparse.parse( sql_statement )[0]
    if isinstance(parsed.tokens[0], Token) \
    and (
        parsed.tokens[0].value.upper() == 'CREATE'
        or parsed.tokens[0].value.upper() == 'INSERT'
    ):
        # Get the name of the table being created
        table_name = next(token.value for token in parsed.tokens if isinstance(token, Identifier))
        # Get all the FROM statements's metadata
        froms = {k: v for d in extract_from_part(parsed) for k, v in d.items()}
        # Get all the JOIN statements's metadata
        joins = list(extract_join_part(parsed))
        # Get all of the comparisons to compare the number of comparisons to the number of JOIN statements
        comparisons = list(extract_comparisons(parsed))
        # When the number of comparisons does not match the number of joins, the parsing was incorrect, raise and exception.
        selects = list(extract_selects(parsed))
        print(f'selects:\n{selects}')
        if len(comparisons) != len(joins):
            raise Exception('Parsing messed up!')
        out = {table_name:{'joins':[], 'selects':selects}}
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
            if 'join_type' in left:
                out[table_name]['joins'].append({
                    'join_type': left['join_type'],
                    'left':{
                        'schema': left['schema'],
                        'table_name': left['table_name'],
                        'column_name': comparison_left_column
                    },
                    'right':{
                        'schema': right['schema'],
                        'table_name': right['table_name'],
                        'column_name': comparison_right_column
                    },
                })
            elif 'join_type' in right:
                out[table_name]['joins'].append({
                    'join_type': right['join_type'],
                    'left':{
                        'schema': left['schema'],
                        'table_name': left['table_name'],
                        'column_name': comparison_left_column
                    },
                    'right':{
                        'schema': right['schema'],
                        'table_name': right['table_name'],
                        'column_name': comparison_right_column
                    },
                })
            else:
                raise Exception('Could not parse Join')

        # pprint(out)


    