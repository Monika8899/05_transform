import time
import psycopg2
import argparse
import csv

class CensusDataLoader:
    def __init__(self, database_name, database_user, database_password, table_name):
        self.db_name = censusdata
        self.db_user = postgres
        self.db_password = Mon@123
        self.table_name = censusdata
        self.datafile = None
        self.create_table = False

    def parse_command_line_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--file", required=True)
        parser.add_argument("-ct", "--create_table", action="store_true")
        args = parser.parse_args()

        self.datafile = args.file
        self.create_table = args.create_table

    def read_data_from_file(self):
        print(f"Reading data from file: {self.datafile}")
        with open(self.datafile, mode="r") as file:
            data_reader = csv.DictReader(file)
            data_rows = [row for row in data_reader]
        return data_rows

    def create_database_table(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(f"""
                DROP TABLE IF EXISTS {self.table_name};
                CREATE TABLE {self.table_name} (
                    CensusTract         NUMERIC,
                    State               TEXT,
                    County              TEXT,
                    TotalPop            INTEGER,
                    Men                 INTEGER,
                    Women               INTEGER,
                    Hispanic            DECIMAL,
                    White               DECIMAL,
                    Black               DECIMAL,
                    Native              DECIMAL,
                    Asian               DECIMAL,
                    Pacific             DECIMAL,
                    Citizen             DECIMAL,
                    Income              DECIMAL,
                    IncomeErr           DECIMAL,
                    IncomePerCap        DECIMAL,
                    IncomePerCapErr     DECIMAL,
                    Poverty             DECIMAL,
                    ChildPoverty        DECIMAL,
                    Professional        DECIMAL,
                    Service             DECIMAL,
                    Office              DECIMAL,
                    Construction        DECIMAL,
                    Production          DECIMAL,
                    Drive               DECIMAL,
                    Carpool             DECIMAL,
                    Transit             DECIMAL,
                    Walk                DECIMAL,
                    OtherTransp         DECIMAL,
                    WorkAtHome          DECIMAL,
                    MeanCommute         DECIMAL,
                    Employed            INTEGER,
                    PrivateWork         DECIMAL,
                    PublicWork          DECIMAL,
                    SelfEmployed        DECIMAL,
                    FamilyWork          DECIMAL,
                    Unemployment        DECIMAL
                );
            """)
            print(f"Created {self.table_name} table")
            connection.commit()

    def load_data_into_database(self, connection, commands):
        with connection.cursor() as cursor:
            print(f"Loading data into {self.table_name} table")
            start_time = time.perf_counter()
            for cmd in commands:
                cursor.execute(cmd)
            elapsed_time = time.perf_counter() - start_time
            print(f'Finished loading data. Elapsed Time: {elapsed_time:0.4} seconds')

    def run(self):
        self.parse_command_line_arguments()
        connection = psycopg2.connect(
            host="localhost",
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )
        data_rows = self.read_data_from_file()
        sql_commands = self.generate_sql_commands(data_rows)

        if self.create_table:
            self.create_database_table(connection)

        self.load_data_into_database(connection, sql_commands)
        connection.close()

    def generate_sql_commands(self, data_rows):
        sql_commands = []
        for row in data_rows:
            values = self.row_to_values(row)
            sql_command = f"INSERT INTO {self.table_name} VALUES ({values});"
            sql_commands.append(sql_command)
        return sql_commands

    def row_to_values(self, row):
        for key in row:
            if not row[key]:
                row[key] = 0
            row['County'] = row['County'].replace('\'', '')
        values = ", ".join([f"'{value}'" if isinstance(value, str) else f"{value}" for value in row.values()])
        return values

if __name__ == "__main__":
    data_loader = CensusDataLoader("censusdata", "postgres", "Mon@123", "censusData")
    data_loader.run()
