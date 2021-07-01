"""Parses a '.sql' file for a set of joins and selects per statement.
"""
import re
import json
import os
import sys
from typing import Union, Tuple
from dotenv import load_dotenv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Token
from sqlparse.tokens import Keyword, DML, Punctuation
import psycopg2

# Load the values found in the local ``.env`` file.
load_dotenv()

PARSE_PATH = os.getenv('PARSE_PATH')
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("REDSHIFT_USER")
PASSWORD = os.getenv("PASSWORD")

try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DATABASE,
        connect_timeout=1
    )
except psycopg2.OperationalError:
    print('Could not connect to Redshift. Bad credentials or not on VPN?')
    sys.exit(1)
cursor = connection.cursor()
tables = []

def found_table(schema:str, table_name:str) -> bool:
    """Returns whether the given table is found in the list of cached tables"""
    return len(
        [
            table for table in tables 
            if table.table_name == table_name and table.schema == schema
        ]
    ) == 1

class Column():
    """Object used to store column metadata"""
    def __init__( #pylint: disable=R0913
        self, column_name:str, data_type:str, data_length:int, is_nullable:str, default_value:str
    ) -> None:
        self._column_name = column_name
        self.data_type = data_type.upper()
        self.data_length = data_length
        self.is_nullable = is_nullable == 'YES'
        self.default_value = default_value

    @property
    def column_name(self) -> str:
        """Gives the name of the table's column"""
        return self._column_name

    def __str__(self) -> str:
        return f'{self._column_name}::{self.data_type}'

    def __repr__(self) -> str:
        return str(self)

    def __iter__(self):
        yield 'name', self._column_name
        yield 'data_type', self.data_type
        yield 'data_length', self.data_length
        yield 'is_nullable', self.is_nullable
        yield 'default_value', self.default_value

class Table():
    """Object used to store table metadata"""
    def __init__(self, schema:str, table_name:str, redshift_cursor, alias=None):
        self.schema = schema
        self.table_name = table_name
        self.redshift_cursor = redshift_cursor
        self.columns = []
        self.has_queried = False
        self.alias = alias
        self.is_temp = False
        if self.schema == self.table_name:
            self.is_temp = True

    def __str__(self) -> str:
        if self.has_queried and not self.is_temp:
            return f'{self.schema}.{self.table_name} with {len(self.columns)} columns'
        elif self.is_temp:
            return f'TEMP {self.table_name}'
        else:
            return f'{self.schema}.{self.table_name} not queried from Redshift'

    def __repr__(self) -> str:
        return str(self)

    def __iter__(self):
        yield 'schema', self.schema
        yield 'name', self.table_name
        yield 'alias', self.alias
        yield 'columns', [dict(column) for column in self.columns]

    def has_column(self, column_name:str) -> bool:
        """Returns whether the table has a column with a specific name"""
        return len([column for column in self.columns if column.column_name == column_name]) == 1

    def get_column(self, column_name:str) -> Column:
        """Returns the column from the table"""
        if not self.has_column(column_name) and not self.is_temp:
            raise Exception(f'`{self.schema}.{self.table_name}` does not have column `{column_name}`')
        if self.is_temp:
            return Column(column_name, 'UNKNOWN', 0, 'UNKNOWN', None)
        else:
            return [column for column in self.columns if column.column_name == column_name][0]


    def query_data(self) -> None:
        """Queries the table's metadata from Redshift"""
        self.has_queried = True
        self.redshift_cursor.execute(
            'SELECT' \
                + ' ordinal_position as position,' \
                + ' column_name,' \
                + ' data_type,' \
                + ' coalesce(character_maximum_length, numeric_precision) as max_length,' \
                + ' is_nullable,' \
                + ' column_default as default_value' \
            + ' FROM information_schema.columns' \
            + ' WHERE' \
                + f' table_name = \'{self.table_name}\'' \
                + f' AND table_schema = \'{self.schema}\'' \
            + ' ORDER BY ordinal_position;'
        )
        # connection.commit()
        self.columns = [
            Column(
                details[1], details[2], details[3], details[4], details[5]
            ) for details in self.redshift_cursor.fetchall()
        ]
        # connection.commit() is this needed??

class JoinComparison():
    """Object used to store the comparison used in a JOIN statement"""
    def __init__(
        self,
        left_column:Union[str, Tuple[Column, Table]],
        right_column:Union[str,Tuple[Column, Table]],
        operator:str
    ) -> None:
        if isinstance(left_column, str):
            self.left = left_column
            self.left_str = True
            self.left_table = None
            self.left_column = None
        else:
            self.left_str = False
            self.left_table = left_column[1]
            self.left_column = left_column[0]
        if isinstance(right_column, str):
            self.right = right_column
            self.right_str = True
            self.right_table = None
            self.right_column = None
        else:
            self.right_str = False
            self.right_table = right_column[1]
            self.right_column = right_column[0]
        self.operator = operator

    def __str__(self) -> str:
        if self.operator == '=':
            _operator = 'equals'
        elif self.operator == '>':
            _operator = 'greater than'
        elif self.operator == '>=':
            _operator = 'greater than or equal to'
        elif self.operator == '<':
            _operator = 'less than'
        elif self.operator == '<=':
            _operator = 'less than or equal to'
        else:
            return f'Cannot find operator: {self.operator}'
        if self.left_str:
            _left = self.left
        else:
            _left = '.'.join([
                self.left_table.schema, self.left_table.table_name, self.left_column.column_name
            ])
        if self.right_str:
            _right = self.right
        else:
            _right = '.'.join([
               self.right_table.schema, self.right_table.table_name, self.right_column.column_name
            ])
        return f'{_left} {_operator} {_right}'

    def __repr__(self):
        return str(self)

class Join():
    """Object used to store a SQL join statement and its comparisons"""
    def __init__(self, join_type:str) -> None:
        self.join_type = join_type
        self.comparisons = []

    def add_comparison(self, comparison:JoinComparison) -> None:
        """Adds a comparison to the list of the Join's comparisons"""
        self.comparisons.append(comparison)

def remove_comments(sql_string:str) -> None:
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
    # Remove the comments from the token.
    sql_no_comments = remove_comments(token.value.strip())
    # Search for all of the ``select`` and ``from`` in this token.
    select_matches = list(re.finditer(r'select\s', sql_no_comments, re.MULTILINE|re.IGNORECASE))
    from_matches = list(re.finditer(r'from\s', sql_no_comments, re.MULTILINE|re.IGNORECASE))
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
    # Get all of the columns used in the SELECT statement by splitting the text between the first
    # ``select`` and ``from``.
    selected_columns = sql_no_comments[select_matches[0].span()[1]:from_matches[0].span()[0]] \
        .split(',')
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
                select_statement += "," + selected_columns[select_index].strip()
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
            # print('-----')
            # print(table_alias)
            # print(column_name)
            # print(aliases)
            # print(select_statement)
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
            # Check to see if the column in being casted into a specific data type
            cast_match = re.match(r'([a-zA-Z0-9_]+)\s*::\s*([a-zA-Z0-9_]+)', select_statement)
            operation = ' '.join(select_statement.split(' ')[:-1])
            column_name = select_statement.split(' ')[-1]
            # Add the table and schema when a single table/schema is being selected from
            if cast_match:
                yield {
                    'schema': list(aliases.values())[0]['schema'],
                    'table_name': list(aliases.values())[0]['table_name'],
                    'column_name': cast_match.groups()[0],
                    'cast_type': cast_match.groups()[1]
                }
            elif len(aliases) == 1:
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

def extract_from_part(token, redshift_cursor):
    """Yields the ``FROM`` portion of a query"""
    from_seen = False
    # Iterate over the differnet tokens
    for _token in token.tokens:
        if _token.is_whitespace:
            continue
        if from_seen:
            if is_subselect(_token):
                for __token in extract_from_part(_token, redshift_cursor):
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
                    this_table = Table(schema, table_real_name, redshift_cursor, alias)
                    this_table.query_data()
                    tables.append(this_table)
                    yield {
                        alias:{
                            'table_name': table_real_name,
                            'schema': schema,
                            'token': _token
                        }
                    }
        if _token.ttype is Keyword and _token.value.upper() == 'FROM':
            from_seen = True

def extract_join_part(token, redshift_cursor):
    """Yields the ``JOIN`` portion of a query"""
    join = None
    join_type = None
    comparisons = False
    for _token in token.tokens:
        # print(isinstance(_token, Comparison))

        # Ingore all whitespace tokens.
        # NOTE: The sqlparse packages considers comparisons as `whitespace`.
        if _token.is_whitespace and not isinstance(_token, Comparison):
            continue

        # print('-----')
        # print(_token.value)
        # print(isinstance(_token, Comparison))
        if comparisons and isinstance(_token, Comparison):
            # Remove the comments from the token
            _token_no_comments = sqlparse.parse(
                sqlparse.format(_token.value, strip_comments=True).strip()
            )[0].tokens[0]
            # print('_token_no_comments.right')
            # print(_token_no_comments.right)
            left_tables = [
                table for table in tables
                if table.alias == str(_token_no_comments.left).split('.')[0]
            ]
            right_tables = [
                table for table in tables
                if table.alias == str(_token_no_comments.right).split('.')[0]
            ]
            # print(f'Number right tables: {len(right_tables)}')
            if len(left_tables) == 1:
                left_table = left_tables[0]
                left_column = left_table.get_column(str(_token_no_comments.left).split('.')[1])
            if len(right_tables) == 1:
                right_table = right_tables[0]
                right_column = right_table.get_column(str(_token_no_comments.right).split('.')[1])
            comparison = JoinComparison(
                (left_column, left_table),
                (right_column, right_table),
                _token_no_comments.value
                    .replace(str(_token_no_comments.left), '')
                    .replace(str(_token_no_comments.right), '')
                    .strip()
            )
            join.add_comparison(comparison)
            # print('comparisons')
            # print(comparison)
            # print(_token_no_comments.value)
        if join_type:
            # TODO: Implement subquery match with pythonic objects
            # Find the different comparisons used in this join. The join type is now known and the
            # comparisons must be set.
            if _token.ttype is Keyword:
                comparisons = True
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
                if not alias in [table.alias for table in tables]:
                    this_table = Table(redshift_schema, table_real_name, redshift_cursor, alias)
                    this_table.query_data()
                    tables.append(this_table)
                    print(f'Appending this table ({this_table.alias}):')
                    print(this_table)
                    print([table.alias for table in tables])
                yield {
                    alias: {
                        'join_type': join_type,
                        'table_name': table_real_name,
                        'schema': redshift_schema,
                        'token': _token
                    }
                }
        if _token.ttype is Keyword and _token.value.upper() in (
            'JOIN',
            'LEFT JOIN',
            'RIGHT JOIN',
            'INNER JOIN',
            'FULL JOIN',
            'LEFT OUTER JOIN',
            'FULL OUTER JOIN'
        ):
            join_type = _token.value.upper()
            join = Join(_token.value.upper())
        print()

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
            column_comparison_match = re.match(
                r'([a-zA-Z_]+)\.([a-zA-Z_]+)\s+=\s+([a-zA-Z_]+)\.([a-zA-Z_]+)',
                token.value
            )
            string_comparison_match = re.match(
                r'([a-zA-Z_]+)\.([a-zA-Z_]+)\s+=\s+\'([\w\W]+)\'',
                token.value
            )
            if column_comparison_match:
                yield (
                    column_comparison_match.groups()[0] \
                        + '.' + column_comparison_match.groups()[1],
                    column_comparison_match.groups()[2] \
                        + '.' + column_comparison_match.groups()[3]
                )
            elif string_comparison_match:
                yield (
                    string_comparison_match.groups()[0] \
                        + '.' + string_comparison_match.groups()[1],
                    string_comparison_match.groups()[2]
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
    # Add the table metadata to the cached tables to access later.
    if len(table_name.split('.')) == 2\
        and not found_table(table_name.split('.')[0], table_name.split('.')[1]):
        this_table = Table(
            table_name.split('.')[0], table_name.split('.')[1], cursor
        )
        print(f'Appending this table ({this_table.alias}):')
        print(this_table)
        this_table.query_data()
        tables.append(this_table)
    elif len(table_name.split('.')) == 3 \
        and not found_table(table_name.split('.')[1], table_name.split('.')[2]):
        this_table = Table(
            table_name.split('.')[1], table_name.split('.')[2], cursor
        )
        print('Appending this table')
        print(this_table)
        this_table.query_data()
        tables.append(this_table)
    # print(this_table)
    # Get all the FROM statements's metadata
    froms = {k: v for d in extract_from_part(parsed, cursor) for k, v in d.items()}
    print('Tables:')
    print([table for table in tables])
    # Get all the JOIN statements's metadata
    joins = list(extract_join_part(parsed, cursor))
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
    # "/Users/tnorlund/etl_aws_copy/apps/dm-tmp-transform-prod/sql/transform.tmp.daily_active_subs_and_frequency_poc.sql"
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
    print('FINISHED STATEMENT')
    tables = []

# print(out)
with open('dmt_f_invoice.json', 'w') as json_file:
    json.dump(out, json_file)
