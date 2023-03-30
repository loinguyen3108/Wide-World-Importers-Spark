from datetime import datetime
from functools import cached_property

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit

from wwi.dependencies.settings import DATALAKE_PATH, HDFS_MASTER, JDBC_MSSQL_DRIVER, JDBC_PASSWORD, JDBC_URL, JDBC_USER
from wwi.services import BaseService
from wwi.services.hdfs.hdfs import HDFSService
from wwi.services.utils import query_by_date, query_by_id

BY_DATE = 'by_date'
BY_ID = 'by_id'


class IngestionService(BaseService):
    def __init__(self) -> None:
        super().__init__()

    @cached_property
    def hdfs_service(self):
        return HDFSService()

    def extract(self, table_name: str, loading_type: str, p_key: str, schema_name: str, ingestion_date: datetime):
        """Extract data from MSSQL.
        
        Args:
            table_name (str): Table Name need extract
            loading_type (str): Type increment loading (Ex: ID, Date or All)
            p_key (str): Column Name to filter
            schema_name (str): Schema Name need extract
            ingest_date (datetime): Ingestion date
            
        Returns:
            DataFrame: Table is extracted
        """
        query_string = f'{schema_name}.{table_name}'
        if self.hdfs_service.is_exists(f'/{DATALAKE_PATH}/{table_name}'):
            df_exists = self.spark.read \
                .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}')
            if loading_type == BY_ID:
                query_string = query_by_id(df=df_exists, table_name=table_name,
                                           p_key=p_key, schema_name=schema_name)
            elif loading_type == BY_DATE:
                query_string = query_by_date(df=df_exists, table_name=table_name,
                                             p_key=p_key, schema_name=schema_name)
        df = self.spark.read.format('jdbc') \
            .option('url', JDBC_URL) \
            .option('user', JDBC_USER) \
            .option('password', JDBC_PASSWORD) \
            .option('dbtable', query_string) \
            .option('driver', JDBC_MSSQL_DRIVER) \
            .option('dateFormat', 'yyyy-MM-dd') \
            .option('fetchsize', 10000) \
            .load()
            
        df = df.withColumn('ingestion_year', lit(ingestion_date.year)) \
            .withColumn('ingestion_month', lit(ingestion_date.month)) \
            .withColumn('ingestion_day', lit(ingestion_date.day))
        return df
    
    def load(self, df: DataFrame, table_name: str):
        """Load data to Data Lake

        Args:
            df (DataFrame): Table is extracted from MSSQL
            table_name (str): Table Name need load into data lake
        """
        file_path = f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}'
        df.write.option('header', True) \
            .partitionBy('ingestion_year', 'ingestion_month', 'ingestion_day') \
            .mode('overwrite') \
            .parquet(file_path)
        self.logger.info(f'Ingest data success in DataLake: {file_path}')
        
    def run(self, table_name: str, loading_type: str, p_key: str, schema_name: str, ingestion_date: datetime):
        # extract data from MSSQL
        df = self.extract(table_name=table_name, loading_type=loading_type, p_key=p_key, 
                          schema_name=schema_name, ingestion_date=ingestion_date)
        
        # load data to datalake
        self.load(df=df, table_name=table_name)
    