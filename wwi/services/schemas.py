from pyspark.sql.types import ArrayType, BooleanType, DateType, FloatType, IntegerType, \
    StringType, StructField, StructType

DIM_CUSTOMER_SCHEMA = StructType([
    StructField(name='CustomerKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='CustomerName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='CustomerCategoryName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='BuyingGroupName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='OpenDateKey', dataType=IntegerType()),
    StructField(name='PhoneNumber', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='FaxNumber', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='WebsiteURL', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='DeliveryCity', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='DeliveryStateProvince', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='DeliveryCountry', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='ExecutionYear', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionMonth', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionDay', dataType=IntegerType(), nullable=False)
    
])

DIM_DELIVERY_METHOD_SCHEMA = StructType([
    StructField(name='DeliveryMethodKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='DeliveryMethodName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='ExecutionYear', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionMonth', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionDay', dataType=IntegerType(), nullable=False)
])

DIM_SALES_PERSON_SCHEMA = StructType([
    StructField(name='SalePersonKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='FullName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='PreferredName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='SearchName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='IsSystemUser', dataType=BooleanType()),
    StructField(name='IsEmployee', dataType=BooleanType()),
    StructField(name='IsSalesPerson', dataType=BooleanType()),
    StructField(name='PhoneNumber', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='FaxNumber', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='EmailAddress', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='ExecutionYear', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionMonth', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionDay', dataType=IntegerType(), nullable=False)
])

DIM_PROMOTION_SCHEMA = StructType([
    StructField(name='PromotionKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='PromotionDescription', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='StartDate', dataType=DateType()),
    StructField(name='EndDate', dataType=DateType()),
    StructField(name='DiscountAmount', dataType=FloatType()),
    StructField(name='DiscountPercentage', dataType=FloatType()),
    StructField(name='ExecutionYear', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionMonth', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionDay', dataType=IntegerType(), nullable=False)
])

DIM_PRODUCT_SCHEMA = StructType([
    StructField(name='ProductKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='ProductName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='SupplierName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='ColorName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='PackageTypeName', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='Brand', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='Size', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='Barcode', dataType=StringType(), metadata={'scale': 0}),
    StructField(name='TaxRate', dataType=FloatType()),
    StructField(name='UnitPrice', dataType=FloatType()),
    StructField(name='RecommendedRetailPrice', dataType=FloatType()),
    StructField(name='TypicalWeightPerUnit', dataType=FloatType()),
    StructField(name='Tags', dataType=ArrayType(StringType())),
    StructField(name='Manufacturer', dataType=StringType()),
    StructField(name='ExecutionYear', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionMonth', dataType=IntegerType(), nullable=False),
    StructField(name='ExecutionDay', dataType=IntegerType(), nullable=False)
])

FACT_SALES_SCHEMA = StructType([
    StructField(name='InvoiceKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='CustomerKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='ProductKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='DeliveryMethodKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='SalesPersonKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='PromotionKey', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='DateKey', dataType=IntegerType()),
    StructField(name='Quantity', dataType=IntegerType(), metadata={'scale': 0}),
    StructField(name='NetUnitPrice', dataType=FloatType()),
    StructField(name='RegularUnitPrice', dataType=FloatType()),
    StructField(name='DiscountUnitPrice', dataType=FloatType()),
    StructField(name='UnitCost', dataType=FloatType()),
    StructField(name='UnitProfit', dataType=FloatType()),
    StructField(name='GrossRevenue', dataType=FloatType()),
    StructField(name='NetRevenue', dataType=FloatType()),
    StructField(name='CostAmount', dataType=FloatType()),
    StructField(name='GrossProfit', dataType=FloatType()),
    StructField(name='NetProfit', dataType=FloatType()),
])
