from helpers.connections import Mysql
import pandas as pd
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
database = Mysql(host='localhost', port='3306', db_name='mysql_db', user_name='root', password='root_mysql')

database.drop_table(table_schema='company', table_name='company_symbols')
database.drop_table(table_schema='company', table_name='company_values')
database.drop_schema(table_schema='company')

database.create_schema('company')

# Reads the data, creates the table and inserts values to company_symbols
company_symbols_df = pd.read_csv(CUR_DIR + "/tutorial_data/Company.csv")
database.create_table(table_schema='company', table_name='company_symbols',
                      columns={'ticker_symbol': 'varchar(20)',
                               'company_name': 'varchar(20)'})
database.insert_values(data=company_symbols_df, table_schema='company', table_name='company_symbols',
                       columns='ticker_symbol, company_name')

# Reads the data, creates the table and inserts values to company_values
company_values_df = pd.read_csv(CUR_DIR + "/tutorial_data/CompanyValues.csv")
database.create_table(table_schema='company', table_name='company_values',
                      columns={'ticker_symbol': 'varchar(20)',
                               'day_date': 'timestamp',
                               'close_value': 'numeric',
                               'volume': 'bigint',
                               'open_value': 'float',
                               'high_value': 'float',
                               'low_value': 'float'})
database.insert_values(data=company_values_df, table_schema='company', table_name='company_values',
                       columns='ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value')

print("Data is ready!")
