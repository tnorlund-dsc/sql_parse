from Column import Column

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

    def get_column(self, column_name:str):
        """Returns the column from the table"""
        if not self.has_column(column_name) and not self.is_temp:
            raise Exception(
                f'`{self.schema}.{self.table_name}` does not have column `{column_name}`'
            )
        if self.is_temp:
            return Column(column_name, 'UNKNOWN', 0, 'UNKNOWN', None, self)
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
                details[1], details[2], details[3], details[4], details[5], self
            ) for details in self.redshift_cursor.fetchall()
        ]
        [_column.get_description() for _column in self.columns]
        return self