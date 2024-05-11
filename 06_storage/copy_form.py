import time
import psycopg2
import argparse
import csv

# Define the database configuration and table details
db_name = "postgres"
db_user = "postgres"
db_password = "postgres"
table_name = 'census_data'
file_path = "AL2015_1.csv"
should_recreate_table = False

def parse_arguments():
    # Handles command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global file_path
    file_path = args.datafile
    global should_recreate_table
    should_recreate_table = args.createtable

def establish_db_connection():
    # Creates and returns a connection to the database
    connection = psycopg2.connect(
        host="localhost",
        database=db_name,
        user=db_user,
        password=db_password,
    )
    connection.autocommit = True
    return connection

def initialize_database_schema(connection):
    # Constructs the initial database schema
    with connection.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
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
                Walk               DECIMAL,
                OtherTransp        DECIMAL,
                WorkAtHome         DECIMAL,
                MeanCommute        DECIMAL,
                Employed           INTEGER,
                PrivateWork        DECIMAL,
                PublicWork         DECIMAL,
                SelfEmployed       DECIMAL,
                FamilyWork         DECIMAL,
                Unemployment       DECIMAL
            );
        """)
        print(f"Database table {table_name} created successfully.")

def load_data_into_database(connection, file):
    # Uses the COPY command to load data efficiently into the database
    with connection.cursor() as cursor, open(file, 'r') as datafile:
        start_time = time.perf_counter()
        next(datafile)  # Skip the header line
        cursor.copy_from(datafile, table_name, sep=',', null='')
        time_elapsed = time.perf_counter() - start_time
        print("Data has been loaded successfully using the COPY command.")
        print(f'Total time to load data: {time_elapsed:.4f} seconds')

def check_table_presence(connection):
    # Checks for the presence of the table in the database
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = '{table_name.lower()}');")
        table_exists = cursor.fetchone()[0]
        if table_exists:
            print(f"The table '{table_name}' exists within the database.")
        else:
            print(f"The table '{table_name}' does not exist in the database.")

def execute_main():
    parse_arguments()
    connection = establish_db_connection()
    if should_recreate_table:
        initialize_database_schema(connection)
    load_data_into_database(connection, file_path)

if _name_ == "_main_":
    execute_main()