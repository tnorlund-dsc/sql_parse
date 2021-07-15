"""Parses a '.sql' file for a set of joins and selects per statement.
"""
import re
import json
import os
import sys
from typing import Union, Tuple
from collections import namedtuple
from dotenv import load_dotenv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Token
from sqlparse.tokens import Keyword, DML, Punctuation
import psycopg2
from pprint import pprint
from parse_types import Column, Table, JoinComparison, Join

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

Subquery = namedtuple('Subquery', 'alias parsedStatement')

class ParsedStatement():
    """Object used to store a parsed SQL statement
    Attributes
    ----------
    tokens : sqlparse.sql.Statement()
        The SQL statement parsed using ``sqlparse``
    table : String or None
        The name of the parsed table
    file_name : str
        The name of the `.sql` file the statement is from
    cursor : sqlparse.connection()
        The ``sqlparse`` database session
    table_cache : list of parse_types.Table()
        A cache used to access stored table data
    selects : list of parse_types.Select()
        The ``SELECT``s used in the SQL statement
    subqueries : list of parse_types.ParsedStatemet()
        The subqueries used in the ``JOIN`` statements
    joins: list of parse_types.JoinComparison()
        The table comparisons used the statement
    """
    def __init__(self, tokens, file_name:str, redshift_cursor) -> None:
        self.tokens = tokens
        self.table = None
        self.file_name = file_name
        self.cursor = redshift_cursor
        self.table_cache = []
        self.selects = []
        self.subqueries = []
        self.joins = []
        self.destination_table = None

    def __str__(self) -> str:
        if self.table is None:
            return 'Still need to parse'
        return f'{self.table.table_name} depends on '

    def __repr__(self) -> str:
        return str(self)

    def __iter__(self):
        yield 'tokens', self.tokens
        yield 'table', self.table
        yield 'file_name', self.file_name
        # yield 'froms', [dict(_from) for _from in self.froms]
        yield 'joins', [dict(_join) for _join in self.joins]

    def has_alias_in_cache(self, alias:str):
        """Returns whether the given table alias is found in the cached tables"""
        return alias in [
            _table.alias for _table in self.table_cache
        ] + [
            _subquery.alias for _subquery in self.subqueries
        ]
    
    def get_alias_in_cache(self, alias:str):
        if alias in [_table.alias for _table in self.table_cache]:
            return [_table for _table in self.table_cache if _table.alias == alias][0]
        if alias in [_subquery.alias for _subquery in self.subqueries]:
            return [_subquery for _subquery in self.subqueries if _subquery.alias == alias][0]

    def _parse_selects(self):
        # Remove the comments from the token.
        sql_no_comments = remove_comments(self.tokens.value.strip())
        # Search for all of the ``select`` and ``from`` in this token.
        select_matches = list(re.finditer(r'select\s', sql_no_comments, re.MULTILINE|re.IGNORECASE))
        from_matches = list(re.finditer(r'from\s', sql_no_comments, re.MULTILINE|re.IGNORECASE))
        # Only use the columns in this SELECT statement. This will be all text between the first
        # ``select`` and ``from`` found in this token.
        if len(select_matches) != len(from_matches):
            raise Exception(
                'The number of SELECTs and JOINs did not match:\n' \
                    + self.tokens.value.strip()
            )
        if len(select_matches) == 0 or len(from_matches) == 0:
            raise Exception(
                'No SELECTs and JOINs found in this token:\n' \
                    + self.tokens.value.strip()
            )
        # Get all of the columns used in the SELECT statement by splitting the text between the
        # first ``select`` and ``from``.
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
                # Get the aliased table or subquery from the parsed statement
                if not self.has_alias_in_cache(table_alias):
                    raise Exception(f'Could not find table with alias {table_alias}')
                _table_or_subquery = self.get_alias_in_cache(table_alias)
                # Save the table
                if isinstance(_table_or_subquery, Table):
                    if not _table_or_subquery.has_column(column_name):
                        raise Exception(
                            f'{_table_or_subquery.table_name} does not have {column_name} as a column'
                        )
                    self.selects.append({
                        'column_name': column_name,
                        'column': _table_or_subquery.get_column(column_name),
                        'table_alias': table_alias,
                        'table': _table_or_subquery
                    })
                # Save the subquery
                else:
                    self.selects.append({
                        'column_name': column_name,
                        'table_alias': table_alias,
                        'subquery': _table_or_subquery
                    })

                
                # _table = [table for table in self.table_cache if table.alias == table_alias][0]

                # print('-----')
                # print(table_alias)
                # print(column_name)
                # print(aliases)
                # print(select_statement)
                # Yield the subquery and the column name when referencing a subquery
                # if self.has_alias_in_cache(table_alias):
                #     print({
                #         'column_name': column_name,
                #         'table_alias': table_alias,
                #         # 'subquery': list(aliases[table_alias]['subquery'].values())[0]
                #     })
                # if 'subquery' in aliases[table_alias].keys():
                    # yield {
                    #     'column_name': column_name,
                    #     'table_alias': table_alias,
                    #     'subquery': list(aliases[table_alias]['subquery'].values())[0]
                    # }
                # else:
                #     print(self.has_alias_in_cache(table_alias))
                #     self.selects.append({
                #         'schema': _table.schema,
                #         'table_name': _table.table_name,
                #         'column_name': column_name,
                #         'column_from': column_name,
                #         'table_alias': table_alias
                #     })
                    # yield {
                    #     'schema': aliases[table_alias]['schema'],
                    #     'table_name': aliases[table_alias]['table_name'],
                    #     'column_name': column_name,
                    #     'column_from': column_name,
                    #     'table_alias': table_alias
                    # }
            elif rename_match_with_as or rename_match_without_as:
                if rename_match_without_as:
                    table_alias = rename_match_without_as.groups()[0]
                    column_from = rename_match_without_as.groups()[1]
                    column_name = rename_match_without_as.groups()[2]
                    _table_or_subquery = self.get_alias_in_cache(table_alias)
                    # Set the 
                    print(self.has_alias_in_cache(table_alias))
                    # Yield the column name and the alias's name when referencing a subquery.
                    # if aliases[table_alias]['schema'][0] == '(':
                    #     yield {
                    #         'column_name': column_name,
                    #         'table_alias': table_alias
                    #     }
                    # else:
                    #     self.selects.append({
                    #         'schema': _table.schema,
                    #         'table_name': _table.table_name,
                    #         'column_name': column_name,
                    #         'column_from': column_from,
                    #         'table_alias': table_alias
                    #     })
                    #     yield {
                    #         'schema': aliases[table_alias]['schema'],
                    #         'table_name': aliases[table_alias]['table_name'],
                    #         'column_name': column_name,
                    #         'column_from': column_from,
                    #         'table_alias': table_alias
                    #     }
                else:
                    table_alias = rename_match_with_as.groups()[0]
                    column_from = rename_match_with_as.groups()[1]
                    column_name = rename_match_with_as.groups()[2]
                    _table_or_subquery = self.get_alias_in_cache(table_alias)
                    # Save the table
                    if isinstance(_table_or_subquery, Table):
                        if not _table_or_subquery.has_column(column_from):
                            raise Exception(
                                f'{_table_or_subquery.table_name} does not have {column_from} as a column'
                            )
                        self.selects.append({
                            'column_name': column_name,
                            'column': _table_or_subquery.get_column(column_from),
                            'table_alias': table_alias,
                            'table': _table_or_subquery
                        })
                    # Save the subquery
                    else:
                        self.selects.append({
                            'column_name': column_name,
                            'table_alias': table_alias,
                            'subquery': _table_or_subquery
                        })
                    print(self.has_alias_in_cache(table_alias))
                    # Yield the subquery and the column name when referencing a subquery
                    # if 'subquery' in aliases[table_alias].keys():
                    #     yield {
                    #         'column_name': column_name,
                    #         'table_alias': table_alias,
                    #         'subquery': list(aliases[table_alias]['subquery'].values())[0]
                    #     }
                    # else:
                    #     yield {
                    #         'schema': aliases[table_alias]['schema'],
                    #         'table_name': aliases[table_alias]['table_name'],
                    #         'column_name': column_name,
                    #         'column_from': column_from,
                    #         'table_alias': table_alias
                    #     }
            # Add the function's call to the select statement
            elif function_match:
                operation = function_match.groups()[0]
                column_name = function_match.groups()[1]
                self.selects.append({
                    'operation': operation, 
                    'column_name':column_name
                })
            else:
                # Check to see if the column in being casted into a specific data type
                cast_match = re.match(r'([a-zA-Z0-9_]+)\s*::\s*([a-zA-Z0-9_]+)', select_statement)
                operation = ' '.join(select_statement.split(' ')[:-1])
                column_name = select_statement.split(' ')[-1]
                print(operation)
                # Add the table and schema when a single table/schema is being selected from
                # if cast_match:
                #     yield {
                #         'schema': list(aliases.values())[0]['schema'],
                #         'table_name': list(aliases.values())[0]['table_name'],
                #         'column_name': cast_match.groups()[0],
                #         'cast_type': cast_match.groups()[1]
                #     }
                # elif len(aliases) == 1:
                #     yield {
                #         'schema': list(aliases.values())[0]['schema'],
                #         'table_name': list(aliases.values())[0]['table_name'],
                #         'column_name': column_name,
                #     }
                # else:
                #     yield {
                #         'operation': operation,
                #         'column_name': column_name
                #     }

    def _parse_table(self):
        print('_parse_table()')
        # Get the name of the table being created
        _table_name = next(token.value for token in self.tokens if isinstance(token, Identifier))
        # Add the table metadata to the cached tables to access later.
        if len(_table_name.split('.')) == 2 \
            and not found_table(_table_name.split('.')[0], _table_name.split('.')[1]):
            _table = Table(
                _table_name.split('.')[0], _table_name.split('.')[1], self.cursor
            ).query_data()
            self.table = _table
            self.table_cache.append(_table)
            self.destination_table = _table
        elif len(_table_name.split('.')) == 3 \
            and not found_table(_table_name.split('.')[1], _table_name.split('.')[2]):
            _table = Table(
                _table_name.split('.')[1], _table_name.split('.')[2], self.cursor
            ).query_data()
            self.table = _table
            self.table_cache.append(_table)
            self.destination_table = _table
        else:
            _table = Table(_table_name, _table_name, self.cursor)
            self.table = _table
            self.table_cache.append(_table)

    def _parse_froms(self, token):
        """Yields the ``FROM`` portion of a query"""
        print('_parse_froms')
        from_seen = False
        # Iterate over the differnet tokens
        for _token in token.tokens:
            if _token.is_whitespace:
                continue
            if from_seen:
                if is_subselect(_token):
                    print('subselect')
                    print(_token)
                    # for __token in extract_from_part(_token, self.cursor):
                        # yield __token
                elif _token.ttype is Keyword or _token.ttype is Punctuation:
                    from_seen = False
                    continue
                else:
                    # The alias used to reference the table in the query
                    alias = _token.get_name()
                    # When the alias is found as `None`, there is no ``FROM`` found in this query.
                    # TODO figure out why this condition is here
                    if alias is None:
                        # return
                        continue
                    # The full table name without the schema
                    table_real_name = _token.get_real_name()
                    # The Redshift schema where the table is accessed from
                    schema = _token.value.replace(f".{table_real_name}", '').split(' ')[0]
                    # When the schema starts with an opening paranthesis, ``(``, there is a subquery
                    # used in this FROM statement. It must be recursively iterated upon.
                    if schema[0] == '(':
                        _subquery = ParsedStatement(
                            sqlparse.parse(
                                re.sub(r'\)\s+' + table_real_name, '', _token.value)[1:]
                            )[0],
                            self.file_name,
                            self.cursor
                        )
                        _subquery.parse()
                        self.subqueries.append(Subquery(table_real_name, _subquery))
                    # Otherwise, the FROM portion of this statement is referencing another table.
                    else:
                        _table = Table(schema, table_real_name, self.cursor, alias)
                        _table.query_data()
                        self.table_cache.append(_table)
                        # self.froms.append(_table)
            if _token.ttype is Keyword and _token.value.upper() == 'FROM':
                from_seen = True

    def _parse_joins(self, token):
        """Yields the ``JOIN`` portion of a query"""
        join = None
        join_type = None
        comparisons = False
        for _token in token.tokens:
            # Ingore all whitespace tokens.
            # NOTE: The sqlparse packages considers comparisons as `whitespace`.
            if _token.is_whitespace and not isinstance(_token, Comparison):
                continue
            # Add the different comparisons used in the join statement
            if comparisons and isinstance(_token, Comparison):
                # Remove the comments from the token
                _token_no_comments = sqlparse.parse(
                    sqlparse.format(_token.value, strip_comments=True).strip()
                )[0].tokens[0]
                left_tables = [
                    _table for _table in self.table_cache
                    if _table.alias == str(_token_no_comments.left).split('.')[0]
                ]
                right_tables = [
                    _table for _table in self.table_cache
                    if _table.alias == str(_token_no_comments.right).split('.')[0]
                ]
                if len(left_tables) == 1:
                    left_table = left_tables[0]
                    left_column = left_table.get_column(
                        str(_token_no_comments.left).split('.')[1]
                    )
                if len(right_tables) == 1:
                    right_table = right_tables[0]
                    right_column = right_table.get_column(
                        str(_token_no_comments.right).split('.')[1]
                    )
                comparison = JoinComparison(
                    (left_column, left_table),
                    (right_column, right_table),
                    _token_no_comments.value
                        .replace(str(_token_no_comments.left), '')
                        .replace(str(_token_no_comments.right), '')
                        .strip()
                )
                join.add_comparison(comparison)
                self.joins.append(join)
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
                    print('MATCHED SUBQUERY!!!')
                    # subquery = parse_statement(
                    #     sqlparse.parse(subquery_match.groups()[0])[0],
                    #     {}
                    # )
                    # print(sqlparse.parse(subquery_match.groups()[0])[0])
                    # print('subquery')
                    # print(subquery)
                    _subquery = ParsedStatement(
                        sqlparse.parse(subquery_match.groups()[0])[0],
                        self.file_name,
                        self.cursor
                    )
                    _subquery.parse()
                    # The alias used to reference the table in the query
                    alias = _token.get_name()
                    if not self.has_alias_in_cache(alias):
                        self.subqueries.append(Subquery(alias, _subquery))
                    # The full table name without the schema
                    table_real_name = _token.get_real_name()
                    # yield {
                    #     alias: {
                    #         'join_type': join_type,
                    #         'subquery': subquery,
                    #         'table_name': table_real_name,
                    #         'token': _token
                    #     }
                    # }
                # Just the alias of the table is given in this token. Store the table and the alias
                # in the object's ``table_cache``.
                else:
                    # The alias used to reference the table in the query
                    alias = _token.get_name()
                    # The full table name without the schema
                    table_real_name = _token.get_real_name()
                    # The Redshift schema where the table is accessed from
                    redshift_schema = _token.value.replace(f".{table_real_name}", '').split(' ')[0]
                    if not self.has_alias_in_cache(alias):
                    # if not alias in [table.alias for table in tables]:
                        _table = Table(redshift_schema, table_real_name, self.cursor, alias)
                        _table.query_data()
                        self.table_cache.append(_table)
                        print(f'Appending this table ({_table.alias}):')
                        # print(this_table)
                        print([_table.alias for _table in self.table_cache])
                    # yield {
                    #     alias: {
                    #         'join_type': join_type,
                    #         'table_name': table_real_name,
                    #         'schema': redshift_schema,
                    #         'token': _token
                    #     }
                    # }
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

    def parse(self) -> None:
        """Parses the SQL statement for dependencies"""
        self._parse_table()
        self._parse_froms(self.tokens)
        self._parse_joins(self.tokens)
        self._parse_selects()

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
            # TODO Write a separate function for parsin the right and left tables. 
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
                print('MATCHED SUBQUERY!!!')
                subquery = parse_statement(
                    sqlparse.parse(subquery_match.groups()[0])[0],
                    {}
                )
                print('subquery')
                print(subquery)
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
# sql_contents = """
# CREATE TEMP TABLE dm_delta AS
# select distinct i.customer_id as customer_id
# from stg.erp_invoices i
#   inner join stg.orders o
#     on i.order_id = o.id
#   left outer join stg.erp_shipments s
#     on i.order_id = s.order_id
# where (
#          i.dsc_processed_at >= '<start_date>'::timestamp  -  interval '1 day'
#          OR o.updated_at >= '<start_date>'::timestamp -  interval '1 day'
#          OR s.dsc_processed_at >= '<start_date>'::timestamp -  interval '1 day'
#        )
# ;
# """
out = {}

for sql_statement in sqlparse.split(sql_contents):
    # Tokenize the SQL statement
    parsed_sql = sqlparse.parse( sql_statement )[0]
    if isinstance(parsed_sql.tokens[0], Token) \
    and (
        parsed_sql.tokens[0].value.upper() == 'CREATE'
        or parsed_sql.tokens[0].value.upper() == 'INSERT'
    ):
        _statement = ParsedStatement(
            parsed_sql,
            '/Users/tnorlund/etl_aws_copy/apps/dm-transform/sql/transform.dmt.f_invoice.sql',
            cursor
        )
        _statement.parse()
        print(_statement)
        # print(type(parsed_sql))
        # out = parse_statement(parsed_sql, out)
    print('FINISHED STATEMENT')
    tables = []

# print(out)
with open('dmt_f_invoice.json', 'w') as json_file:
    json.dump(out, json_file)
