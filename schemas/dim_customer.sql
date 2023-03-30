CREATE TABLE IF NOT EXISTS WWI.DimCustomer (
    CustomerKey INT NOT NULL,
    CustomerName STRING NOT NULL,
    CustomerCategory STRING,
    BuyingGroup STRING,
    OpenDateKey INT NOT NULL,
    PhoneNumber STRING,
    FaxNumber STRING,
    WebsiteURL STRING,
    DeliveryCity STRING NOT NULL,
    DeliveryProvince STRING NOT NULL,
    DeliveryCountry STRING NOT NULL
)
COMMENT 'Customer Information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
