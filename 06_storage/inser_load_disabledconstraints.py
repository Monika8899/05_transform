import time
import psycopg2
import argparse
import csv

# Database configuration details
database_name = "postgres"
database_user = "postgres"
database_password = "postgres"
table_name = 'CensusData'
data_file = "AL2015_1.csv"
should_create_db = False


def setup_configuration():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global data_file
    data_file = args.datafile
    global should_create_db
    should_create_db = args.createtable


def format_row_to_values(row):
    for key in row:
        if not row[key]:
            row[key] = 0
        row['County'] = row['County'].replace('\'', '')

    formatted_values = f"""
       {row['CensusTract']},            
       '{row['State']}',                
       '{row['County']}',               
       {row['TotalPop']},               
       {row['Men']},                    
       {row['Women']},                  
       {row['Hispanic']},               
       {row['White']},                  
       {row['Black']},                  
       {row['Native']},                 
       {row['Asian']},                  
       {row['Pacific']},                
       {row['Citizen']},                
       {row['Income']},                 
       {row['IncomeErr']},              
       {row['IncomePerCap']},           
       {row['IncomePerCapErr']},        
       {row['Poverty']},                
       {row['ChildPoverty']},           
       {row['Professional']},           
       {row['Service']},                
       {row['Office']},                 
       {row['Construction']},           
       {row['Production']},             
       {row['Drive']},                  
       {row['Carpool']},                
       {row['Transit']},                
       {row['Walk']},                   
       {row['OtherTransp']},            
       {row['WorkAtHome']},             
       {row['MeanCommute']},            
       {row['Employed']},               
       {row['PrivateWork']},            
       {row['PublicWork']},             
       {row['SelfEmployed']},           
       {row['FamilyWork']},             
       {row['Unemployment']}            
    """

    return formatted_values


def establish_database_connection():
    connection = psycopg2.connect(
        host="localhost",
        database=database_name,
        user=database_user,
        password=database_password,
    )
    connection.autocommit = True
    return connection


def construct_table(connection):
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
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork         DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork         DECIMAL,
                Unemployment        DECIMAL
            );
        """)
        print(f"Table {table_name} successfully created.")


def parse_data_from_file(file_name):
    print(f"Processing data from: {file_name}")
    with open(file_name, mode="r") as file:
        data_reader = csv.DictReader(file)

        data_rows = []
        for row in data_reader:
            data_rows.append(row)

    return data_rows


def generate_insert_commands(data_rows):
    command_list = []
    for data in data_rows:
        values_str = format_row_to_values(data)
        sql_command = f"INSERT INTO {table_name} VALUES ({values_str});"
        command_list.append(sql_command)
    return command_list


def load_data_into_db(connection, commands):
    with connection.cursor() as cursor:
        print(f"Starting data load of {len(commands)} entries.")
        start_time = time.perf_counter()

        for command in commands:
            cursor.execute(command)

        duration = time.perf_counter() - start_time
        print(f'Finished loading. Time taken: {duration:0.4f} seconds.')


def main():
    setup_configuration()
    connection = establish_database_connection()
    if should_create_db:
        construct_table(connection)

    data_rows = parse_data_from_file(data_file)
    insert_commands = generate_insert_commands(data_rows)
    load_data_into_db(connection, insert_commands)


if _name_ == "_main_":
    main()