class Column():
    """Object used to store column metadata"""
    def __init__( #pylint: disable=R0913
        self, column_name:str, data_type:str, data_length:int, is_nullable:str, default_value:str, table:any
    ) -> None:
        self._column_name = column_name
        self.data_type = data_type.upper()
        self.data_length = data_length
        self.is_nullable = is_nullable == 'YES'
        self.default_value = default_value
        self.table = table
        self.description = None

    @property
    def column_name(self) -> str:
        """Gives the name of the table's column"""
        return self._column_name

    def __str__(self) -> str:
        if self.description is not None:
            return f'{self._column_name}::{self.data_type} \'{self.description}\''
        return f'{self._column_name}::{self.data_type}'

    def __repr__(self) -> str:
        return str(self)

    def __iter__(self):
        yield 'name', self._column_name
        yield 'data_type', self.data_type
        yield 'data_length', self.data_length
        yield 'is_nullable', self.is_nullable
        yield 'default_value', self.default_value
        if self.description is not None:
            yield 'description', self.description

    def get_description(self):
        self.table.redshift_cursor.execute(
            'SELECT description FROM pg_catalog.pg_description WHERE objsubid=' \
            + '(' \
                + 'SELECT' \
                    + ' ordinal_position' \
                + ' FROM'
                    + ' information_schema.columns'
                + ' WHERE' \
                    + ' table_name=\'{self.table.schema}.{self.table.table_name}\'' \
                    + ' AND' \
                    + ' column_name=\'{self.column_name}\'' \
            + ')' \
            + ' AND objoid=' \
            + '(' \
                + 'SELECT' \
                    + ' oid' \
                + ' FROM' \
                    + ' pg_class' \
                + ' WHERE'\
                    + ' relname=\'{self.table.schema}.{self.table.table_name}\'' \
                    + ' AND' \
                    + ' relnamespace = ' \
                    + '(' \
                        + 'SELECT' \
                            + ' oid' \
                        + ' FROM' \
                            + ' pg_catalog.pg_namespace'
                        + ' WHERE' \
                        + ' nspname=\'public\'' \
                    + ')' \
            + ');'
        )
        self.description = self.table.redshift_cursor.fetchone()
    
    def set_description(self, description:str):
        """Set the PostgreSQL comment associated with this Column
        Paramaters
        ----------
        description : str
            The description of the Column found in the Table.
        """
        self.table.redshift_cursor.execute(
            'COMMENT' \
            + ' ON' \
            + ' COLUMN' \
            + f'{self.table.schema}.{self.table.table_name}.{self.column_name}' \
            + 'IS' \
            + '\'{description}\''
        )
        _result = self.table.redshift_cursor.fetchone()
        print(_result)