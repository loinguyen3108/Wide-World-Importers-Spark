CREATE TABLE IF NOT EXISTS WWI.DimSalesPerson (
    SalesPersonKey INT NOT NULL,
    FullName STRING NOT NULL,
    PreferredName STRING,
    SearchName String,
    IsSystemUser BOOLEAN,
    IsEmployee BOOLEAN,
    IsSalesPerson BOOLEAN,
    PhoneNumber STRING,
    FaxNumber STRING,
    EmailAddress STRING
)
COMMENT 'SalesPerson Information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
