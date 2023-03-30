from os import environ

# Hadoop Env Var
HDFS_URL = environ.get('HDFS_URL', 'http://localhost:9870')
HDFS_MASTER = environ.get('HDFS_MASTER', 'hdfs://localhost:9000')
DATALAKE_PATH = 'user/datalake/WWI'

# Spark Env Var
APP_NAME = 'wwi_app'
JAR_PACKAGES = []
SPARK_FILES = []
SPARK_CONFIGS = {
    'spark.sql.warehouse.dir': f'{HDFS_MASTER}/user/hive/warehouse'
}
SPARK_MASTER = 'spark://loinguyen:7077'
JDBC_MSSQL_DRIVER = environ.get('JDBC_MSSQL_DRIVER', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
JDBC_URL = environ.get('JDBC_URL', 'jdbc:sqlserver://localhost:1401;databaseName=WideWorldImporters;encrypt=true;trustServerCertificate=true;')
JDBC_USER = environ.get('JDBC_USER', 'sa')
JDBC_PASSWORD = environ.get('JDBC_PASSWORD', '<YourNewStrong!Passw0rd>')
