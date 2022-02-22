from helpers.connections import Postgresql
import pandas as pd
from datetime import datetime
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
database = Postgresql(host='localhost', port='5433', db_name='postgres_db', user_name='postgres', password='postgres')

# initialize destination tables for data ingestion
database.create_schema('company')
database.create_table(table_schema='company', table_name='company_symbols',
                      columns={'ticker_symbol': 'varchar',
                               'company_name': 'varchar'})
database.create_table(table_schema='company', table_name='company_values',
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

company_symbol_dict = {'insert_date': str(datetime.now()), 'source_connection': 'mysql',
                       'source_schema': 'company',
                       'source_table': 'company_symbols', 'key_fields': 'ticker_symbol, company_name',
                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 'destination_connection': 'postgresql',
                       'destination_schema': 'company',
                       'destination_table': 'company_symbols', 'target_fields': 'ticker_symbol, company_name'}

company_values_dict = {'insert_date': str(datetime.now()), 'source_connection': 'mysql',
                       'source_schema': 'company',
                       'source_table': 'company_values',
                       'key_fields': 'ticker_symbol, day_date, close_value, volume, '
                                     'open_value, high_value, low_value',
                       'extraction_method': 'jdbc',
                       'extraction_type': 'full', 'destination_connection': 'postgresql',
                       'destination_schema': 'company',
                       'destination_table': 'company_values',
                       'target_fields': 'ticker_symbol, day_date, close_value, volume, '
                                        'open_value, high_value, low_value'}

company_symbol_dict = {k: [v, ] for k, v in company_symbol_dict.items()}
company_values_dict = {k: [v, ] for k, v in company_values_dict.items()}

company_symbol_df = pd.DataFrame(company_symbol_dict)
company_values_df = pd.DataFrame(company_values_dict)

database.insert_values(data=company_symbol_df, table_schema='etl_manager', table_name='database_flow_reference_table',
                       columns=', '.join(company_symbol_df.columns.tolist()))
database.insert_values(data=company_values_df, table_schema='etl_manager', table_name='database_flow_reference_table',
                       columns=', '.join(company_values_df.columns.tolist()))

database.close_connection()