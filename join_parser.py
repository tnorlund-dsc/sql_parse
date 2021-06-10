#!/usr/bin/env python3

# Blatantly stolen from:
# https://www.programmersought.com/article/85254754672/
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
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


file_path = "/Users/tnorlund/etl_aws_copy/apps/dm-transform/sql/transform.dmt.f_invoice.sql"
sql_contents = open(file_path).read()

for sql_statement in sqlparse.split( sql_contents ):
    # Tokenize the SQL statement
    parsed = sqlparse.parse( sql_statement )[0]
    # print(list(extract_table_identifiers(extract_from_part(parsed))))
    # print(list(extract_table_identifiers(extract_join_part(parsed))))
    print(
        [token.value for token in extract_join_part(parsed) if isinstance(token, Identifier)]
    )