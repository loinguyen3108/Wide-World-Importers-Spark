CREATE TABLE IF NOT EXISTS WWI.FactSales (
    InvoiceKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    DeliveryMethodKey INT NOT NULL,
    SalesPersonKey INT NOT NULL,
    PromotionKey INT,
    DateKey INT NOT NULL,
    Quantity INT NOT NULL,
    NetUnitPrice FLOAT NOT NULL,
    RegularUnitPrice FLOAT NOT NULL,
    DiscountUnitPrice FLOAT NOT NULL,
    UnitCost FLOAT NOT NULL,
    UnitProfit FLOAT NOT NULL,
    GrossRevenue FLOAT NOT NULL,
    NetRevenue FLOAT NOT NULL,
    CostAmount FLOAT NOT NULL,
    GrossProfit FLOAT NOT NULL,
    NetProfit FLOAT NOT NULL
)
COMMENT 'Invoice Information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';