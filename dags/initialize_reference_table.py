from helpers.connections import Postgresql
import pandas as pd
from datetime import datetime
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
database = Postgresql(host='localhost', port='5433', db_name='postgres_db', user_name='postgres', password='postgres')

# initialize destination tables for data ingestion
database.create_schema('stock')
database.create_table(table_schema='stock', table_name='stock_symbols',
                      columns={'ticker_symbol': 'varchar',
                               'stock_name': 'varchar'})
database.create_table(table_schema='stock', table_name='stock_values',
                      columns={'ticker_symbol': 'varchar',
                               'day_date': 'timestamp',
                               'close_value': 'numeric',
                               'volume': 'bigint',
                               'open_value': 'float',
                               'high_value': 'float',
                               'low_value': 'float'})

# initialize reference table
database.create_schema('etl_manager')
database.create_table(table_schema='etl_manager', table_name='database_flow_reference_table',
                      columns={'insert_date': 'timestamp',
                               'source_connection': 'varchar',
                               'source_schema': 'varchar',
                               'source_table': 'varchar',
                               'key_fields': 'varchar',
                               'extraction_method': 'varchar',
                               'extraction_type': 'varchar',
                               'destination_connection': 'varchar',
                               'destination_schema': 'varchar',
                               'destination_table': 'varchar',
                               'target_fields': 'varchar'})

database.truncate_table(table_schema='etl_manager', table_name='database_flow_reference_table')

stock_symbol_dict = {'insert_date': str(datetime.now()), 'source_connection': 'mysql',
                       'source_schema': 'stock',
                       'source_table': 'stock_symbols', 'key_fields': 'ticker_symbol, stock_name',
                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 'destination_connection': 'postgresql',
                       'destination_schema': 'stock',
                       'destination_table': 'stock_symbols', 'target_fields': 'ticker_symbol, stock_name'}

stock_values_dict = {'insert_date': str(datetime.now()), 'source_connection': 'mysql',
                       'source_schema': 'stock',
                       'source_table': 'stock_values',
                       'key_fields': 'ticker_symbol, day_date, close_value, volume, '
                                     'open_value, high_value, low_value',
                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 'destination_connection': 'postgresql',
                       'destination_schema': 'stock',
                       'destination_table': 'stock_values',
                       'target_fields': 'ticker_symbol, day_date, close_value, volume, '
                                        'open_value, high_value, low_value'}

stock_symbol_dict = {k: [v, ] for k, v in stock_symbol_dict.items()}
stock_values_dict = {k: [v, ] for k, v in stock_values_dict.items()}

stock_symbol_df = pd.DataFrame(stock_symbol_dict)
stock_values_df = pd.DataFrame(stock_values_dict)

database.insert_values(data=stock_symbol_df, table_schema='etl_manager', table_name='database_flow_reference_table',
                       columns=', '.join(stock_symbol_df.columns.tolist()))
database.insert_values(data=stock_values_df, table_schema='etl_manager', table_name='database_flow_reference_table',
                       columns=', '.join(stock_values_df.columns.tolist()))

database.close_connection()