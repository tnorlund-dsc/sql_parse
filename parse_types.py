from typing import Union, Tuple

class Select():
    def __init__(
        self, column_name: str
    ) -> None:
        self._column_name = column_name

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
    """Object used to store table metadata
    
    Attributes
    ----------
    schema : str
        The Redshift schema used to access the table
    table_name : str
        The name of the parsed table
    redshift_cursor : sqlparse.connection()
        The ``sqlparse`` database session
    columns : list of Column()
        The columns found after querying Redshift
    has_queried : bool
        Whether the table has been queried from Redshift
    alias : str
        The table's alias used in the SQL statement
    is_temp : bool
        Whether the table is a temporary DB table
    """
    def __init__(self, schema:str, table_name:str, redshift_cursor, alias=None):
        """The initialization of the Table object.

        Parameters
        ----------
        schema : str
            The Redshift schema used to access the table
        table_name : str
            The name of the parsed table
        redshift_cursor : sqlparse.connection()
            The ``sqlparse`` database session
        alias : str, default to None
            The table's alias used in the SQL statement
        """
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
            if self.schema is None:
                return f'{self.schema}.{self.table_name} with {len(self.columns)} columns'    
            return f'{self.alias} {self.schema}.{self.table_name} with {len(self.columns)} columns'
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
            raise Exception(
                f'`{self.schema}.{self.table_name}` does not have column `{column_name}`'
            )
        if self.is_temp:
            return Column(column_name, 'UNKNOWN', 0, 'UNKNOWN', None)
        return [column for column in self.columns if column.column_name == column_name][0]

    def query_data(self):
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
        self.columns = [
            Column(
                details[1], details[2], details[3], details[4], details[5]
            ) for details in self.redshift_cursor.fetchall()
        ]
        return self

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

    def __iter__(self):
        if isinstance(self.left_str, str):
            yield 'left', self.left
        else:
            yield 'left', {'table': self.left_table, 'column':self.left_column}
        if isinstance(self.right_str, str):
            yield 'right', self.right
        else:
            yield 'right', {'table': self.right_table, 'column':self.right_column}
        yield 'operator', self.operator

class Join():
    """Object used to store a SQL join statement and its comparisons"""
    def __init__(self, join_type:str) -> None:
        self.join_type = join_type
        self.comparisons = []
    
    def __iter__(self):
        yield 'type', self.join_type
        yield 'comparisons', [dict(_comparison) for _comparison in self.comparisons]

    def add_comparison(self, comparison:JoinComparison) -> None:
        """Adds a comparison to the list of the Join's comparisons"""
        self.comparisons.append(comparison)