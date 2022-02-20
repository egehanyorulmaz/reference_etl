"""
Created by: Egehan Yorulmaz (egehanyorulmaz@gmail.com)
Created at: 02/19/2022
"""
import os
import psycopg2
import pandas as pd
from mysql import connector
from pandas import DataFrame
from datetime import datetime

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


class Database:
    def __init__(self, database_type: str, host: str, port: str, db_name: str, user_name: str, password: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self._user_name = user_name
        self._password = password
        self.database_type = database_type
        self.conn = self.establish_connection()
        self.cursor = self.conn.cursor()

    def establish_connection(self):
        """
        Create connection to local Database.
        :return: Connection object
        """
        try:
            if self.database_type == 'postgresql':
                conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.db_name, user=self._user_name,
                                        password=self._password)
                conn.set_session(autocommit=True)
            elif self.database_type == 'mysql':
                conn = connector.connect(host=self.host,
                                         port=self.port,
                                         database=self.db_name,
                                         user=self._user_name,
                                         password=self._password)
                conn.autocommit = True
            print(f'Successfully connected to {self.db_name}!')

            return conn
        except:
            print(
                f'Error when connecting to {self.database_type} database! Please check your docker container and make '
                f'sure database is running and you have given the correct informations!')

    def execute_query(self, query: str, return_data=True):
        """
        Creates cursor object, executes query. If return_date is True, fetchall and converts to pandas dataframe.
        :param query: Sql query to be executed
        :param return_data: whether given query is wanted to be fetched and returned as pandas dataframe.
        :return: Pandas.Dataframe if return_data is True
        """
        try:
            self.cursor.execute(query)
            print("Query successful")
            if return_data:
                columns = [desc[0] for desc in self.cursor.description]
                data_df = pd.DataFrame(self.cursor.fetchall(), columns=columns)
                return data_df

        except Exception as e:
            self.cursor.execute("ROLLBACK")
            self.close_connection()
            print(f"Error when executing query: '{e}'")
            raise Exception("There is a problem with your query. Please control it. Marking task as failed! ")

    def truncate_table(self, table_schema: str, table_name: str) -> None:
        query = f"TRUNCATE TABLE {table_schema}.{table_name}"
        print(query)
        self.execute_query(query, return_data=False)

    def create_schema(self, table_schema: str) -> None:
        query = f"CREATE SCHEMA IF NOT EXISTS {table_schema};"
        self.execute_query(query, return_data=False)
        print(query)

    def create_table(self, table_schema: str, table_name: str, columns: dict) -> None:
        query = f"CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} ("
        modified_dict = [f"{column_name} {column_datatype}" for column_name, column_datatype in columns.items()]
        column_informations = ",\n".join(modified_dict)
        query += column_informations
        query += ');'
        self.execute_query(query, return_data=False)
        print(query)

    def drop_schema(self, table_schema: str) -> None:
        query = f"DROP SCHEMA IF EXISTS {table_schema};"
        self.execute_query(query, return_data=False)

    def drop_table(self, table_schema: str, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {table_schema}.{table_name};"
        self.execute_query(query, return_data=False)

    def insert_values(self, data: DataFrame, table_schema: str, table_name: str, columns: str) -> None:
        """
        Inserts value to database for specified schema and table in the ETL pipeline.
        """
        print("Columns to be inserted: ", str(columns))
        print("Columns in the data extracted: ", str(data.columns.tolist()))

        data.reset_index(drop=True, inplace=True)
        insert_str = f""" INSERT INTO {table_schema}.{table_name} ({columns}) \n VALUES """
        for idx1, row in data.iterrows():
            insert_row = list(row)
            modified_insertion_values = "("

            for idx2, x in enumerate(insert_row):
                if pd.isna(x):  # if data is empty, then Null value will be inserted
                    insertion_substr = "Null"
                elif isinstance(x, pd.Timestamp):  # if data is type of timestamp, then 'x' is added to timestamp
                    insertion_substr = f"'{x}'"
                elif isinstance(x, str):  # if data is type of string, ''x'' is added since string can contain '
                    x = x.replace("'", "''")
                    insertion_substr = f"'{x}'"
                else:
                    insertion_substr = str(x)

                modified_insertion_values += insertion_substr
                if idx2 + 1 != len(insert_row):
                    modified_insertion_values += ', '

            if idx1 + 1 == len(data):
                modified_insertion_values += ');'
            else:
                modified_insertion_values += '), '

            insert_str += modified_insertion_values

        print('SQL query is generated at', str(datetime.now()), sep=" ")
        print("START OF QUERY:")
        print(insert_str[:1000])
        print("END OF QUERY:")
        print(insert_str[-1000:])
        self.execute_query(insert_str, return_data=False)
        print('Ingestion process has completed!')

    def close_connection(self):
        """
        Terminates connection to the database
        """
        self.conn.close()
        self.cursor.close()
        print("Connection is successfully terminated!")


class Mysql(Database):
    def __init__(self, host: str, port: str, db_name: str, user_name: str, password: str):
        super().__init__('mysql', host, port, db_name, user_name, password)


class Postgresql(Database):
    def __init__(self, host: str, port: str, db_name: str, user_name: str, password: str):
        super().__init__('postgresql', host, port, db_name, user_name, password)
