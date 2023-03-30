CREATE TABLE IF NOT EXISTS WWI.DimPromotion (
    PromotionKey INT NOT NULL,
    PromotionDescription STRING NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    DiscountAmount Float,
    DiscountPercentage INT
)
COMMENT 'Promotion Information'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
