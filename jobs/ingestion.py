from argparse import ArgumentParser
from datetime import datetime

from wwi.services.ingestion import IngestionService


arg_parser = ArgumentParser(description='Data Ingestion')
arg_parser.add_argument(
    '--ingestion-date', dest='ingestion_date',
    type=str, help='Date Format: YYYY-MM-DD.')
arg_parser.add_argument(
    '--loading-type', dest='loading_type',
    type=str, help='Choice: [by_date, by_id, all].')
arg_parser.add_argument(
    '--p-key', dest='p_key',
    type=str, help='Column name filter.')
arg_parser.add_argument(
    '--table-name', dest='table_name',
    type=str, help='Table need ingest.')
arg_parser.add_argument(
    '--schema-name', dest='schema_name',
    type=str, help='Schema need ingest.')
args = arg_parser.parse_args()
ingestion_date = datetime.fromisoformat(args.ingestion_date)
loading_type = args.loading_type
p_key = args.p_key
table_name = args.table_name
schema_name = args.schema_name

ingestion = IngestionService()
ingestion.run(table_name=table_name, p_key=p_key, schema_name=schema_name, 
              loading_type=loading_type, ingestion_date=ingestion_date)
