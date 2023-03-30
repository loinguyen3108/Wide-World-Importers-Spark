from argparse import ArgumentParser
from datetime import datetime

from wwi.services.transform import TransformService


arg_parser = ArgumentParser(description='Data ETL')
arg_parser.add_argument('--init-dim-date', dest='init_dim_date', action='store_true', 
                        help='Init DimDate')
arg_parser.add_argument('--ingestion-date', dest='ingestion_date', type=str, 
                        default=str(datetime.now().date()),  help='Date Format: YYYY-MM-DD.')
arg_parser.add_argument('--exec-date', dest='exec_date', type=str, 
                        default=str(datetime.now().date()), help='Date Format: YYYY-MM-DD.')
args = arg_parser.parse_args()
init_dim_date = args.init_dim_date
ingestion_date = datetime.fromisoformat(args.ingestion_date)
exec_date = datetime.fromisoformat(args.exec_date)

etl = TransformService(ingestion_date=ingestion_date, exec_date=exec_date)
if init_dim_date and exec_date:
    etl.init_dim_date()
else:
    etl.run()
