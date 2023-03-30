CREATE TABLE IF NOT EXISTS WWI.DimDeliveryMethod (
    DeliveryMethodKey INT NOT NULL,
    DeliveryMethodName String NOT NULL
)
COMMENT 'Delivery Method Information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
