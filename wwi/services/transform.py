from datetime import datetime

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, dayofmonth, dayofweek, dayofyear, from_json,\
    lit, month, quarter, year
from pyspark.sql.types import ArrayType, BooleanType, DateType, FloatType, IntegerType, \
    StringType, StructField, StructType

from wwi.dependencies.settings import DATALAKE_PATH, HDFS_MASTER
from wwi.services import BaseService
from wwi.services.schemas import DIM_CUSTOMER_SCHEMA, DIM_DELIVERY_METHOD_SCHEMA, \
    DIM_PRODUCT_SCHEMA, DIM_PROMOTION_SCHEMA, DIM_SALES_PERSON_SCHEMA, FACT_SALES_SCHEMA

# Table name
BUYING_GROUPS = 'BuyingGroups'
CUSTOMERS = 'Customers'
CUSTOMER_CATEGORIES = 'CustomerCategories'
CITIES = 'Cities'
COUNTRIES = 'Countries'
COLORS = 'Colors'
DELIVERY_METHODS = 'DeliveryMethods'
INVOICES = 'Invoices'
INVOICE_LINES = 'InvoiceLines'
STATE_PROVINCES = 'StateProvinces'
SPECIAL_DEALS = 'SpecialDeals'
PACKAGE_TYPES = 'PackageTypes'
PEOPLE = 'People'
STOCK_ITEMS = 'StockItems'
SUPPLIERS = 'Suppliers'

# DWH tables
WWI_DB = 'wwi'
DIM_CUSTOMER = 'DimCustomer'
DIM_DELIVERY_METHOD = 'DimDeliveryMethod'
DIM_DATE = 'DimDate'
DIM_SALES_PERSON = 'DimSalesPerson'
DIM_PROMOTION = 'DimPromotion'
DIM_PRODUCT = 'DimProduct'
FACT_SALES = 'FactSales'


# Table fields
BUYING_GROUPS_FIELDS = ['BuyingGroupID', 'BuyingGroupName']
CUSTOMER_FIELDS = ['CustomerID', 'CustomerName', 'CustomerCategoryID', 'BuyingGroupID',
                   'AccountOpenedDate', 'PhoneNumber', 'FaxNumber', 'WebsiteURL',
                   'DeliveryCityID']
CUSTOMER_CATEGORY_FIELDS = ['CustomerCategoryID', 'CustomerCategoryName']
CITY_FIELDS = ['CityID', 'CityName', 'StateProvinceID']
COUNTRY_FIELDS = ['CountryID', 'CountryName']
COLOR_FILEDS = ['ColorID', 'ColorName']
DELIVERY_METHOD_FIELDS = ['DeliveryMethodID', 'DeliveryMethodName']
INVOICE_FIELDS = ['InvoiceID', 'CustomerID', 'DeliveryMethodID', 'SalespersonPersonID', 'InvoiceDate']
INVOICE_LINE_FIELDS = ['InvoiceLineID', 'InvoiceID', 'StockItemID', 'PackageTypeID', 'Quantity',
                       'UnitPrice', 'TaxRate', 'TaxAmount', 'LineProfit', 'ExtendedPrice']
STATE_PROVINCE_FIELDS = ['StateProvinceID', 'StateProvinceName', 'CountryID']
SPECIAL_DEAL_FIELDS = ['SpecialDealID', 'BuyingGroupID', 'DealDescription', 'StartDate', 'EndDate',
                       'DiscountAmount', 'DiscountPercentage']
PACKAGE_TYPE_FIELDS = ['PackageTypeID', 'PackageTypeName']
PEOPLE_FIELDS = ['PersonID', 'FullName', 'PreferredName', 'SearchName', 'IsSystemUser', 
                 'IsEmployee', 'IsSalesperson', 'PhoneNumber', 'FaxNumber', 'EmailAddress']
STOCK_ITEM_FIELDS = ['StockItemID', 'StockItemName', 'SupplierID', 'ColorID', 'UnitPackageID', 
                     'Brand', 'Size', 'Barcode', 'TaxRate', 'UnitPrice', 'RecommendedRetailPrice',
                     'TypicalWeightPerUnit', 'Tags', 'QuantityPerOuter', 'CustomFields']
SUPPLIER_FIELDS = ['SupplierID', 'SupplierName']


class TransformService(BaseService):
    def __init__(self, ingestion_date: datetime, exec_date: datetime) -> None:
        super().__init__(enable_hive=True)
        self.ingestion_date = ingestion_date or datetime.now()
        self.exec_date = exec_date or datetime.now()
        
    def extract(self):
        """Extract data from data lake"""
        # Sales schema
        self.customer = self._extract(CUSTOMERS, is_filter=True).select(CUSTOMER_FIELDS).alias('c')
        self.customer_category = self._extract(CUSTOMER_CATEGORIES) \
            .select(CUSTOMER_CATEGORY_FIELDS).alias('cc')
        self.buying_group = self._extract(BUYING_GROUPS).select(BUYING_GROUPS_FIELDS) \
            .alias('bg')
        self.special_deals = self._extract(SPECIAL_DEALS, is_filter=True).select(SPECIAL_DEAL_FIELDS) \
            .alias('sd')
        self.invoices = self._extract(INVOICES, is_filter=True).select(INVOICE_FIELDS).alias('invoice')
        self.invoice_lines = self._extract(INVOICE_LINES).select(INVOICE_LINE_FIELDS).alias('il')
        
        # Application schema
        self.city = self._extract(CITIES).select(CITY_FIELDS).alias('city')
        self.delivery_method = self._extract(DELIVERY_METHODS).select(DELIVERY_METHOD_FIELDS).alias('dm')
        self.state_province = self._extract(STATE_PROVINCES).select(STATE_PROVINCE_FIELDS) \
            .alias('sp')
        self.country = self._extract(COUNTRIES).select(COUNTRY_FIELDS).alias('country')
        self.person = self._extract(PEOPLE, is_filter=True).select(PEOPLE_FIELDS).alias('ps')
        
        # Warehouse schema
        self.stock_items = self._extract(STOCK_ITEMS, is_filter=True).select(STOCK_ITEM_FIELDS).alias('si')
        
        # Parchasing schema
        self.colors = self._extract(COLORS).select(COLOR_FILEDS).alias('color')
        self.suppliers = self._extract(SUPPLIERS, is_filter=True).select(SUPPLIER_FIELDS).alias('sl')
        self.package_types = self._extract(PACKAGE_TYPES).select(PACKAGE_TYPE_FIELDS) \
            .alias('pt')
        
        
    def _extract(self, table_name: str, is_filter: bool = False) -> DataFrame:
        df = self.spark.read.parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}')
        if is_filter:
            df = df.filter(
                (col('ingestion_year') == self.ingestion_date.year) &
                (col('ingestion_month') == self.ingestion_date.month) &
                (col('ingestion_day') == self.ingestion_date.day))
        return df
    
    def _extract_hive(self, tb_name: str):
        return self.spark.read.table(tb_name).localCheckpoint()
    
    def transforms(self):
        self.dim_customer = self._transform_dim_customer()
        self.dim_delivery_method = self._transform_dim_delivery_method()
        self.dim_sales_person = self._transform_dim_sales_person()
        self.dim_promotion = self._transform_dim_promotion()
        self.dim_product = self._transform_dim_product()
        self.fact_sales = self._transform_fact_sales()
    
    def load(self):
        self._load(df=self.dim_customer, tb_name=DIM_CUSTOMER, save_mode='overwrite',
                   partition_keys=['ExecutionYear', 'ExecutionMonth', 'ExecutionDay'])
        self._load(df=self.dim_delivery_method, tb_name=DIM_DELIVERY_METHOD,
                   save_mode='overwrite', partition_keys=['ExecutionYear', 'ExecutionMonth', 'ExecutionDay'])
        self._load(df=self.dim_sales_person, tb_name=DIM_SALES_PERSON, save_mode='overwrite',
                   partition_keys=['ExecutionYear', 'ExecutionMonth', 'ExecutionDay'])
        self._load(df=self.dim_promotion, tb_name=DIM_PROMOTION, save_mode='overwrite',
                   partition_keys=['ExecutionYear', 'ExecutionMonth', 'ExecutionDay'])
        self._load(df=self.dim_product, tb_name=DIM_PRODUCT, save_mode='overwrite',
                   partition_keys=['ExecutionYear', 'ExecutionMonth', 'ExecutionDay'])
        self._load(df=self.fact_sales, tb_name='test_fact_sales', tb_format='parquet', partition_keys=['DateKey'])
    
    def run(self):
        self.extract()
        self.transforms()
        self.load()
    
    def _load(self, df: DataFrame, tb_name: str, tb_format: str = 'hive', save_mode: str = 'append', 
              partition_keys: list = []):
        self.logger.info('Loading...')
        df.write.option('header', True) \
            .format(tb_format) \
            .partitionBy(partition_keys) \
            .mode(save_mode) \
            .saveAsTable(f'{WWI_DB}.{tb_name}')
        self.logger.info(
            f'Load data success in DWH: {WWI_DB}.{tb_name}.')
    
    def init_dim_date(self):
        beign_date = '1750-01-01'
        df = self.spark.sql(
            f"select explode(sequence(to_date('{beign_date}'), to_date('{self.exec_date.date()}'), interval 1 day)) as date")
        df = df.withColumn('date_id', (year('date') * 10000 + month('date') * 100 + \
                                        dayofmonth('date')).cast(IntegerType())) \
            .withColumn('year', year('date')) \
            .withColumn('month', year('date')) \
            .withColumn('day', year('date')) \
            .withColumn('quarter', quarter('date')) \
            .withColumn('day_of_week', dayofweek('date')) \
            .withColumn('day_of_month', dayofmonth('date')) \
            .withColumn('day_of_year', dayofyear('date'))
            
        self._load(df=df, tb_name=DIM_DATE, save_mode='overwrite')
        
    def _transform_dim_customer(self) -> DataFrame:
        customer_address = self._get_address()
        
        # join dataframe
        cc_condition = col('c.CustomerCategoryID') == col('cc.CustomerCategoryID')
        bg_condition = col('c.BuyingGroupID') == col('bg.BuyingGroupID')
        address_condition = col('c.DeliveryCityID') == col('city.CityID')
        join_df = self.customer \
            .join(other=self.customer_category, on=cc_condition, how='left') \
            .join(other=self.buying_group, on=bg_condition, how='left') \
            .join(other=customer_address, on=address_condition, how='left')
        
        # transform data
        transform_df = join_df.withColumnRenamed('CustomerID', 'CustomerKey') \
            .withColumnRenamed('CityName', 'DeliveryCity') \
            .withColumnRenamed('StateProvinceName', 'DeliveryStateProvince') \
            .withColumnRenamed('CountryName', 'DeliveryCountry') \
            .withColumn('OpenDateKey', 
                        (year('c.AccountOpenedDate') * 10000 + month('c.AccountOpenedDate') * 100  \
                        + dayofmonth('c.AccountOpenedDate')).cast(IntegerType())) \
            .withColumn('ExecutionYear', lit(self.exec_date.year)) \
            .withColumn('ExecutionMonth', lit(self.exec_date.month)) \
            .withColumn('ExecutionDay', lit(self.exec_date.day))
        
        # select fields
        final_df = transform_df.select(DIM_CUSTOMER_SCHEMA.fieldNames())
        
        # validate schema
        final_df = self.validate_schema(final_df, expected_schema=DIM_CUSTOMER_SCHEMA)
        if self.spark.catalog.tableExists(tableName=DIM_CUSTOMER, dbName=WWI_DB):
            src_tb = self._extract_hive(tb_name=f'{WWI_DB}.{DIM_CUSTOMER}')
            return self.merge_into(source_table=src_tb, new_table=final_df,  p_key='CustomerKey')
        return final_df
    
    def _transform_dim_delivery_method(self) -> DataFrame:
        # transform data
        transform_df = self.delivery_method.withColumnRenamed('DeliveryMethodID', 'DeliveryMethodKey') \
            .withColumn('ExecutionYear', lit(self.exec_date.year)) \
            .withColumn('ExecutionMonth', lit(self.exec_date.month)) \
            .withColumn('ExecutionDay', lit(self.exec_date.day))
        
        # select fields
        final_df = transform_df.select(DIM_DELIVERY_METHOD_SCHEMA.fieldNames())
        
        # validate schema
        final_df = self.validate_schema(final_df, expected_schema=DIM_DELIVERY_METHOD_SCHEMA)
        
        if self.spark.catalog.tableExists(tableName=DIM_DELIVERY_METHOD, dbName=WWI_DB):
            src_tb = self._extract_hive(tb_name=f'{WWI_DB}.{DIM_DELIVERY_METHOD}')
            return self.merge_into(source_table=src_tb, new_table=final_df,  p_key='DeliveryMethodKey')
        return final_df
    
    def _transform_dim_sales_person(self) -> DataFrame:
        # transform data
        transform_df = self.person.withColumnRenamed('PersonID', 'SalePersonKey') \
            .withColumn('IsSystemUser', col('IsSystemUser').cast(BooleanType())) \
            .withColumn('IsEmployee', col('IsEmployee').cast(BooleanType())) \
            .withColumn('IsSalesPerson', col('IsSalesPerson').cast(BooleanType())) \
            .withColumn('ExecutionYear', lit(self.exec_date.year)) \
            .withColumn('ExecutionMonth', lit(self.exec_date.month)) \
            .withColumn('ExecutionDay', lit(self.exec_date.day))
        
        # select fields
        final_df = transform_df.select(DIM_SALES_PERSON_SCHEMA.fieldNames())
        
        # validate schema
        final_df = self.validate_schema(final_df, expected_schema=DIM_SALES_PERSON_SCHEMA)
        
        if self.spark.catalog.tableExists(tableName=DIM_SALES_PERSON, dbName=WWI_DB):
            src_tb = self._extract_hive(tb_name=f'{WWI_DB}.{DIM_SALES_PERSON}')
            return self.merge_into(source_table=src_tb, new_table=final_df,  p_key=' SalesPersonKey')
        return final_df
    
    def _transform_dim_promotion(self) -> DataFrame:
        # transform data
        transform_df = self.special_deals.withColumnRenamed('SpecialDealID', 'PromotionKey') \
            .withColumnRenamed('DealDescription', 'PromotionDescription') \
            .withColumn('StartDate', col('StartDate').cast(DateType())) \
            .withColumn('EndDate', col('EndDate').cast(DateType())) \
            .withColumn('DiscountAmount', col('DiscountAmount').cast(FloatType())) \
            .withColumn('DiscountPercentage', col('DiscountPercentage').cast(FloatType())) \
            .withColumn('ExecutionYear', lit(self.exec_date.year)) \
            .withColumn('ExecutionMonth', lit(self.exec_date.month)) \
            .withColumn('ExecutionDay', lit(self.exec_date.day))
        
        # select fields
        final_df = transform_df.select(DIM_PROMOTION_SCHEMA.fieldNames())
        
        # validate schema
        final_df = self.validate_schema(final_df, expected_schema=DIM_PROMOTION_SCHEMA)
        
        if self.spark.catalog.tableExists(tableName=DIM_PROMOTION, dbName=WWI_DB):
            src_tb = self._extract_hive(tb_name=f'{WWI_DB}.{DIM_PROMOTION}')
            return self.merge_into(source_table=src_tb, new_table=final_df,  p_key=' PromotionKey')
        return final_df
    
    def _transform_dim_product(self) -> DataFrame:
        # join dataframe
        
        supplier_condition = col('si.SupplierID') == col('sl.SupplierID')
        color_condition = col('si.ColorID') == col('color.ColorID')
        pt_condition = col('si.UnitPackageID') == col('pt.PackageTypeID')
        join_df = self.stock_items \
            .join(other=self.suppliers, on=supplier_condition, how='left') \
            .join(other=self.colors, on=color_condition, how='left') \
            .join(other=self.package_types, on=pt_condition, how='left')
        
        # transform data
        customer_fields_schema = StructType([
            StructField(name='CountryOfManufacture', dataType=StringType()),
            StructField(name='Tags', dataType=ArrayType(StringType()))
        ])
        transform_df = join_df.withColumnRenamed('StockItemID', 'ProductKey') \
            .withColumnRenamed('StockItemName', 'ProductName') \
            .withColumn('TaxRate', col('TaxRate').cast(FloatType())) \
            .withColumn('UnitPrice', col('UnitPrice').cast(FloatType())) \
            .withColumn('RecommendedRetailPrice', col('RecommendedRetailPrice').cast(FloatType())) \
            .withColumn('TypicalWeightPerUnit', col('TypicalWeightPerUnit').cast(FloatType())) \
            .withColumn('QuantityPerOuter', col('QuantityPerOuter').cast(IntegerType())) \
            .withColumn('CustomFields', from_json(col('CustomFields'), schema=customer_fields_schema)) \
            .withColumn('Manufacturer', col('CustomFields.CountryOfManufacture')) \
            .withColumn('Tags', col('CustomFields.Tags').cast(ArrayType(StringType()))) \
            .withColumn('ExecutionYear', lit(self.exec_date.year)) \
            .withColumn('ExecutionMonth', lit(self.exec_date.month)) \
            .withColumn('ExecutionDay', lit(self.exec_date.day))
            
        # select fields
        final_df = transform_df.select(DIM_PRODUCT_SCHEMA.fieldNames())
        
        # validate schema
        final_df = self.validate_schema(final_df, expected_schema=DIM_PRODUCT_SCHEMA)
        
        if self.spark.catalog.tableExists(tableName=DIM_PRODUCT, dbName=WWI_DB):
            src_tb = self._extract_hive(tb_name=f'{WWI_DB}.{DIM_PRODUCT}')
            return self.merge_into(source_table=src_tb, new_table=final_df,  p_key=' ProductKey')
        return final_df
    
    def _transform_fact_sales(self) -> DataFrame:
        # join dataframe
        invoice_customer_condition = col('invoice.CustomerID') == col('c.CustomerID')
        il_invoice_condition = col('invoice.InvoiceID') == col('il.InvoiceID')
        invoice_dm_condition = col('invoice.DeliveryMethodID') == col('dm.DeliveryMethodID')
        invoice_ps_condition = col('invoice.SalespersonPersonID') == col('ps.PersonID')
        il_si_condition = col('il.StockItemID') == col('si.StockItemID')
        c_sd_condition = col('c.BuyingGroupID') == col('sd.BuyingGroupID')
        join_df = self.invoice_lines \
            .join(other=self.invoices, on=il_invoice_condition, how='left') \
            .join(other=self.customer, on=invoice_customer_condition, how='left') \
            .join(other=self.delivery_method, on=invoice_dm_condition, how='left') \
            .join(other=self.person, on=invoice_ps_condition, how='left') \
            .join(other=self.stock_items, on=il_si_condition, how='left') \
            .join(other=self.special_deals, on=c_sd_condition, how='left') \
            .drop(col('il.InvoiceID')) \
            .drop(col('invoice.CustomerID')) \
            .drop(col('il.StockItemID')) \
            .drop(col('invoice.DeliveryMethodID'))
        
        # transform data
        transform_df = join_df \
            .withColumnRenamed('InvoiceID', 'InvoiceKey') \
            .withColumnRenamed('CustomerID', 'CustomerKey') \
            .withColumnRenamed('StockItemID', 'ProductKey') \
            .withColumnRenamed('DeliveryMethodID', 'DeliveryMethodKey') \
            .withColumnRenamed('SalespersonPersonID', 'SalesPersonKey') \
            .withColumnRenamed('SpecialDealID', 'PromotionKey') \
            .withColumn('GrossRevenue', col('ExtendedPrice').cast(FloatType())) \
            .withColumn('DateKey', (year('InvoiceDate') * 10000 + month('InvoiceDate') * 100 + \
                                    dayofmonth('InvoiceDate')).cast(IntegerType())) \
            .withColumn('NetUnitPrice', col('il.UnitPrice').cast(FloatType())) \
            .withColumn('RegularUnitPrice', col('si.UnitPrice').cast(FloatType())) \
            .withColumn('DiscountUnitPrice', (col('RegularUnitPrice') - col('NetUnitPrice')).cast(FloatType())) \
            .withColumn('UnitProfit', (col('LineProfit') / col('Quantity')).cast(FloatType())) \
            .withColumn('UnitCost', (col('NetUnitPrice') - col('UnitProfit')).cast(FloatType())) \
            .withColumn('NetRevenue', (col('NetUnitPrice') * col('Quantity')).cast(FloatType())) \
            .withColumn('CostAmount', (col('UnitCost') * col('Quantity')).cast(FloatType())) \
            .withColumn('GrossProfit', (col('GrossRevenue') - col('CostAmount')).cast(FloatType())) \
            .withColumn('NetProfit', (col('NetRevenue') - col('CostAmount')).cast(FloatType()))
            
        # select fields
        final_df = transform_df.select(FACT_SALES_SCHEMA.fieldNames())
        
        # validate schema
        final_df = self.validate_schema(final_df, expected_schema=FACT_SALES_SCHEMA)
        return final_df
        
            
    def _get_address(self):
        sp_condition = col('city.StateProvinceID') == col('sp.StateProvinceID')
        country_condition = col('sp.CountryID') == col('country.CountryID')
        df = self.city.join(self.state_province, sp_condition, 'inner') \
                    .join(self.country, country_condition, 'inner')
        return df
