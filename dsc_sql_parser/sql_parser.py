import click
import json
from sql_metadata import Parser


class ParseSQL():
    def __init__(self,
                 filepath: str):
        """
        :param filepath: file-path the sql file
        """
        self.filepath = filepath

        with open(filepath) as f:
            self.sql_stmnt = f.read()
            self.parsed_sql = Parser(self.sql_stmnt)

    def get_column_metadata(self):
        return json.dumps(self.parsed_sql.columns_dict)

@click.command()
@click.option("--filepath", help="path the json file containing the sql statement")
def run(filepath: str):
    parser_obj = ParseSQL(filepath=filepath)
    print(parser_obj.get_column_metadata())

if __name__ == "__main__":
    run()