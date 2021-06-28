"""Parses a '.sql' file for a set of joins and selects per statement.
"""
import re
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Token
from sqlparse.tokens import Keyword, DML, Punctuation

def remove_comments(sql_string):
    """Removes all comments from the given SQL string"""
    return '\n'.join([
        re.sub(r'\-\-[\s0-9a-zA-Z_\.,\\\/\(\)\':=<>+\-*]*$', '', select_line)
        for select_line in sql_string.split('\n')
    ])

def extract_selects(token, aliases):
    """Gets all the columns selected in a ``SELECT ... FROM`` SQL statement.

    Parameters
    ----------
    token: str
    aliases: dict
    """
    # Search for all of the ``select`` and ``from`` in this token.
    select_matches = list(re.finditer(r'select', token.value.strip(), re.MULTILINE|re.IGNORECASE))
    from_matches = list(re.finditer(r'from\s', token.value.strip(), re.MULTILINE|re.IGNORECASE))
    # Only use the columns in this SELECT statement. This will be all text between the first
    # ``select`` and ``from`` found in this token.
    if len(select_matches) != len(from_matches):
        raise Exception(
            'The number of SELECTs and JOINs did not match:\n{}'.format(token.value.strip())
        )
    if len(select_matches) == 0 or len(from_matches) == 0:
        raise Exception(
            'No SELECTs and JOINs found in this token:\n{}'.format(token.value.strip())
        )
    # Remove the comments from the text between the first ``select`` and ``from``.
    selected_columns = remove_comments(
        token.value.strip()[select_matches[0].span()[1]:from_matches[0].span()[0]]
    ).split(',')
    # Use a list and index to iterate over the different select statements.
    select_index = 0
    selects_out = []
    # Iterate over the different selected columns and group them together by ensuring they
    # maintain the same number of opening and closing paranthesis.
    while select_index < len(selected_columns):
        select_statement = selected_columns[select_index].strip()
        if select_statement.count('(') != select_statement.count(')'):
            while select_statement.count('(') != select_statement.count(')'):
                select_index += 1
                select_statement += ", " + selected_columns[select_index].strip()
            selects_out.append(select_statement)
            select_index += 1
        else:
            selects_out.append(
                ' '.join([line.strip() for line in select_statement.split('\n')])
            )
            select_index += 1
    # Iterate over the different select statements to find how the column is used
    for select_statement in selects_out:
        # Find the select statements that have just the schema and the column name from the
        # origin table.
        same_name_match = re.match(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)$', select_statement)
        # Find the select statements with the schema, the column name, and this column's
        # aliased name with the keyword ``as```.
        rename_match_with_as = re.match(
            r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s+as\s+([a-zA-Z0-9_]+)$',
            select_statement,
            re.IGNORECASE
        )
        # Find the select statements with the schema, the column name, and this column's
        # aliased name without the ``as`` keyword.
        rename_match_without_as = re.match(
            r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+)$', select_statement
        )
        # Find the functions applied to the column, aliased with another column name with the
        # keyword ``as``.
        function_match = re.search(
            r'([\w\W]+)\s+as\s+([a-zA-Z0-9_]+)$', select_statement, re.MULTILINE|re.IGNORECASE
        )
        if same_name_match:
            table_alias = same_name_match.groups()[0]
            column_name = same_name_match.groups()[1]
            # Yield the subquery and the column name when referencing a subquery
            if 'subquery' in aliases[table_alias].keys():
                yield {
                    'column_name': column_name,
                    'table_alias': table_alias,
                    'subquery': list(aliases[table_alias]['subquery'].values())[0]
                }
            else:
                yield {
                    'schema': aliases[table_alias]['schema'],
                    'table_name': aliases[table_alias]['table_name'],
                    'column_name': column_name,
                    'column_from': column_name,
                    'table_alias': table_alias
                }
        elif rename_match_with_as or rename_match_without_as:
            if rename_match_without_as:
                table_alias = rename_match_without_as.groups()[0]
                column_from = rename_match_without_as.groups()[1]
                column_name = rename_match_without_as.groups()[2]
                # Yield the column name and the alias's name when referencing a subquery.
                if aliases[table_alias]['schema'][0] == '(':
                    yield {
                        'column_name': column_name,
                        'table_alias': table_alias
                    }
                else:
                    yield {
                        'schema': aliases[table_alias]['schema'],
                        'table_name': aliases[table_alias]['table_name'],
                        'column_name': column_name,
                        'column_from': column_from,
                        'table_alias': table_alias
                    }
            else:
                table_alias = rename_match_with_as.groups()[0]
                column_from = rename_match_with_as.groups()[1]
                column_name = rename_match_with_as.groups()[2]
                # Yield the subquery and the column name when referencing a subquery
                if 'subquery' in aliases[table_alias].keys():
                    yield {
                        'column_name': column_name,
                        'table_alias': table_alias,
                        'subquery': list(aliases[table_alias]['subquery'].values())[0]
                    }
                else:
                    yield {
                        'schema': aliases[table_alias]['schema'],
                        'table_name': aliases[table_alias]['table_name'],
                        'column_name': column_name,
                        'column_from': column_from,
                        'table_alias': table_alias
                    }
        elif function_match:
            operation = function_match.groups()[0]
            column_name = function_match.groups()[1]
            yield {
                'operation': operation,
                'column_name': column_name
            }
        else:
            operation = ' '.join(select_statement.split(' ')[:-1])
            column_name = select_statement.split(' ')[-1]
            # Add the table and schema when a single table/schema is being selected from
            if len(aliases) == 1:
                yield {
                    'schema': list(aliases.values())[0]['schema'],
                    'table_name': list(aliases.values())[0]['table_name'],
                    'column_name': column_name,
                }
            else:
                yield {
                    'operation': operation,
                    'column_name': column_name
                }

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
            elif _token.ttype is Keyword or _token.ttype is Punctuation:
                from_seen = False
                continue
            else:
                # The alias used to reference the table in the query
                alias = _token.get_name()
                # When the alias is found as `None`, there is no ``FROM`` found in this query.
                if alias is None:
                    return
                # The full table name without the schema
                table_real_name = _token.get_real_name()
                # The Redshift schema where the table is accessed from
                schema = _token.value.replace(f".{table_real_name}", '').split(' ')[0]
                # When the schema starts with an opening paranthesis, ``(``, there is a subquery
                # used in this FROM statement. It must be recursively iterated upon.
                if schema[0] == '(':
                    sub_query = parse_statement(
                        sqlparse.parse(
                            re.sub(r'\)\s+' + table_real_name, '', _token.value)[1:]
                        )[0],
                        {}
                    )
                    # When there are more than 1 values found in this recursive step, the parsing
                    # failed.
                    if len(sub_query.values()) > 1:
                        raise Exception(f'Error parsing subquery:\n{_token.value}')
                    yield {
                        _token.get_name():{
                            'subquery':  list(sub_query.values())[0],
                            'token': _token
                        }
                    }
                # Otherwise, the FROM portion of this statement is referencing another table.
                else:
                    yield {
                        alias:{
                            'table_name': table_real_name,
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
            # Continue over this iteration if the token is a SQL keyword.
            if _token.ttype is Keyword:
                join_type = None
                continue
            # Match the value found to see if there is a JOIN using a subquery
            subquery_match = re.match(
                r"\(([\w\W]+)\)", _token.value[:-len(_token.get_name())], re.MULTILINE
            )
            # Yield the subquery output when necessary
            if subquery_match:
                subquery = parse_statement(
                    sqlparse.parse(subquery_match.groups()[0])[0],
                    {}
                )
                # The alias used to reference the table in the query
                alias = _token.get_name()
                # The full table name without the schema
                table_real_name = _token.get_real_name()
                yield {
                    alias: {
                        'join_type': join_type,
                        'subquery': subquery,
                        'table_name': table_real_name,
                        'token': _token
                    }
                }
            else:
                # The alias used to reference the table in the query
                alias = _token.get_name()
                # The full table name without the schema
                table_real_name = _token.get_real_name()
                # The Redshift schema where the table is accessed from
                redshift_schema = _token.value.replace(f".{table_real_name}", '').split(' ')[0]
                yield {
                    alias: {
                        'join_type': join_type,
                        'table_name': table_real_name,
                        'schema': redshift_schema,
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
    """Prints tokens and types to console"""
    for token in list(token):
        if token.is_whitespace:
            continue
        print('----')
        print(token)
        print(type(token))

def extract_comparisons(token):
    """Gets all comparisons from a parsed SQL statement"""
    for token in token.tokens:
        if isinstance(token, Comparison):
            match = re.match(
                r'([a-zA-Z_]+)\.([a-zA-Z_]+)\s+=\s+([a-zA-Z_]+)\.([a-zA-Z_]+)',
                token.value
            )
            if match:
                yield (
                    match.groups()[0] + '.' + match.groups()[1],
                    match.groups()[2] + '.' + match.groups()[3]
                )
            else:
                raise Exception(f'Could not find comparisons:\n{token.value}')

def encode_table(joins, froms, table_name, selects, comparisons, output):
    """Encodes the joins, froms, and """
    output[table_name] = {'joins':[], 'selects':selects}
    # Set the join meta-data
    for index, _ in enumerate(joins):
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
        # Add the subquery to the output when there is one.
        if 'subquery' in right.keys() or 'subquery' in left.keys():
            if 'subquery' in right.keys():
                if 'join_type' in left:
                    output[table_name]['joins'].append({
                        'join_type': left['join_type'],
                        'left':{
                            'schema': left['schema'],
                            'table_name': left['table_name'],
                            'column_name': comparison_left_column
                        },
                        'right':{
                            'subquery': list(right['subquery'].values())[0],
                            'table_name': right['table_name']
                        }
                    })
                elif 'join_type' in right:
                    output[table_name]['joins'].append({
                        'join_type': right['join_type'],
                        'left':{
                            'schema': left['schema'],
                            'table_name': left['table_name'],
                            'column_name': comparison_left_column
                        },
                        'right':{
                            'subquery': list(right['subquery'].values())[0],
                            'table_name': right['table_name']
                        }
                    })
                else:
                    raise Exception('Could not parse Join')
            elif 'subquery' in left.keys():
                if 'join_type' in left:
                    output[table_name]['joins'].append({
                        'join_type': left['join_type'],
                        'left':{
                            'subquery': list(left['subquery'].values)[0],
                            'table_name': left['table_name']
                        },
                        'right':{
                            'schema': right['schema'],
                            'table_name': right['table_name'],
                            'column_name': comparison_right_column
                        }
                    })
                elif 'join_type' in right:
                    output[table_name]['joins'].append({
                        'join_type': right['join_type'],
                        'left':{
                            'subquery': list(left['subquery'].values())[0],
                            'table_name': left['table_name']
                        },
                        'right':{
                            'schema': right['schema'],
                            'table_name': right['table_name'],
                            'column_name': comparison_right_column
                        }
                    })
                else:
                    raise Exception('Could not parse Join')
            else:
                raise Exception('Could not parse joins')
        elif 'join_type' in left:
            output[table_name]['joins'].append({
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
            output[table_name]['joins'].append({
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
    return output

def parse_statement(parsed, output):
    """Parses a tokenized sql_parse token and returns an encoded table."""
    # Get the name of the table being created
    table_name = next(token.value for token in parsed.tokens if isinstance(token, Identifier))
    # Get all the FROM statements's metadata
    froms = {k: v for d in extract_from_part(parsed) for k, v in d.items()}
    # Get all the JOIN statements's metadata
    joins = list(extract_join_part(parsed))
    # Get all of the comparisons to compare the number of comparisons to the number of JOIN
    # statements
    comparisons = list(extract_comparisons(parsed))
    # Get all the columns selected by this query. The table aliases are used to identify where
    # the columns originate from.
    selects = list(
        extract_selects(parsed, {**froms, **{k: v for d in joins for k, v in d.items()}})
    )
    # When the number of comparisons does not match the number of joins, the parsing was
    # incorrect, raise and exception.
    if len(comparisons) != len(joins):
        raise Exception('Parsing messed up!')
    return encode_table(joins, froms, table_name, selects, comparisons, output)

sql_contents = open(
    "/Users/tnorlund/etl_aws_copy/apps/dm-transform/sql/transform.dmt.f_invoice.sql"
    # "/Users/tnorlund/etl_aws_copy/apps/dm-extract/sql/load.stg.erp_invoices.sql"
    # "/Users/tnorlund/etl_aws_copy/apps/dm-erp-transform/sql/transform.spectrum.erp_invoices.sql"
).read()
out = {}

for sql_statement in sqlparse.split(sql_contents):
    # Tokenize the SQL statement
    parsed_sql = sqlparse.parse( sql_statement )[0]
    if isinstance(parsed_sql.tokens[0], Token) \
    and (
        parsed_sql.tokens[0].value.upper() == 'CREATE'
        or parsed_sql.tokens[0].value.upper() == 'INSERT'
    ):
        out = parse_statement(parsed_sql, out)

# print(out)
with open('dmt_f_invoice.json', 'w') as json_file:
    json.dump(out, json_file)
