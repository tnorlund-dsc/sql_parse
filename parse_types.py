from typing import Union, Tuple
from Column import Column
from Table import Table

class Select():
    def __init__(
        self, column_name: str
    ) -> None:
        self._column_name = column_name




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
            yield 'left', {'table': dict(self.left_table), 'column':dict(self.left_column)}
        if isinstance(self.right_str, str):
            yield 'right', self.right
        else:
            yield 'right', {'table': dict(self.right_table), 'column':dict(self.right_column)}
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