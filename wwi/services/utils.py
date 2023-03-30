from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, to_date


def query_by_date(df: DataFrame, table_name: str, p_key: str, schema_name: str):
    """Generate query to extract by latest date (incremental loading by latest date)

    Args:
        df (DataFrame): Spark DataFrame
        table_name (str): Source table
        p_key (str): Key Name to filter
        schema (str): Schema Name from source

    Returns:
        str: Query string to extract table
    """
    max_date = df.select(
        max(to_date(col(p_key))).alias('MaxDate')).collect()[0]['MaxDate']
    return f'(select * from {schema_name}.{table_name} where date({p_key}) > date({max_date})) as tmp'


def query_by_id(df: DataFrame, table_name: str, p_key: str, schema_name: str):
    """Generate query to extract by latest id (incremental loading by latest id)

    Args:
        df (DataFrame): Spark DataFrame
        table_name (str): Source table
        p_key (str): Column Name to filter
        schema (str): Schema Name from source

    Returns:
        str: Query string to extract table
    """
    max_id = df.select(
        max(col(p_key)).alias('MaxID')).collect()[0]['MaxID']
    return f'(select * from {schema_name}.{table_name} where {p_key} > {max_id}) as tmp'
