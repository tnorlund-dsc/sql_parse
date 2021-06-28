"""Parses a '.sql' file for a set of joins and selects per statement.
"""
from join_parser import parse_statement
import re
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison, Token
from sqlparse.tokens import Keyword, DML, Punctuation

class Dependency(object):
    """Class used to find the SQL dependencies in a given ``.sql`` file."""
    def __init__(self, file_name:str):
        self.file_name = file_name

    def parse_statement(self, parsed_sql):
        """Parses a tokenized sql_parse token and returns an encoded table."""
        # Get the name of the table being created
        table_name = next(
            token.value for token in parsed_sql.tokens if isinstance(token, Identifier)
        )
        print(table_name)


    def parse(self):
        """Parses the ``.sql`` file for dependencies."""
        # Read the ``.sql`` file for its contents.
        sql_contents = open(self.file_name).read()
        # Split the file into its individual statements
        for sql_statement in sqlparse.split(sql_contents):
            # Tokenize the SQL statement
            parsed_sql = sqlparse.parse(sql_statement)[0]
            if isinstance(parsed_sql.tokens[0], Token) \
            and (
                parsed_sql.tokens[0].value.upper() == 'CREATE'
                or parsed_sql.tokens[0].value.upper() == 'INSERT'
            ):
                self.parse_statement(parsed_sql)
        # print(self.file_name)
