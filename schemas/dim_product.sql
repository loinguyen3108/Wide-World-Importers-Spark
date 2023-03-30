CREATE TABLE IF NOT EXISTS WWI.DimProduct (
    ProductKey INT NOT NULL,
    ProductName STRING NOT NULL,
    SupplierName STRING NOT NULL,
    ColorName STRING,
    PackageTypeName STRING NOT NULL,
    Brand STRING,
    Size STRING,
    Barcode STRING,
    TaxRate INT NOT NULL,
    UnitPrice FLOAT NOT NULL,
    RecommendRetailPrice FLOAT NOT NULL,
    TypicalWeightPerUnit FLOAT NOT NULL,
    Tags STRING
)
COMMENT 'Product Information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
