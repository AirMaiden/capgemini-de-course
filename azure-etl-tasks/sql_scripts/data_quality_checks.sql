-- Check for NULL or invalid values in critical fields
SELECT 
    COUNT(*) AS InvalidRecords,
    SUM(CASE WHEN price IS NULL OR price <= 0 THEN 1 ELSE 0 END) AS InvalidPrice,
    SUM(CASE WHEN minimum_nights IS NULL OR minimum_nights < 0 THEN 1 ELSE 0 END) AS InvalidMinimumNights,
    SUM(CASE WHEN availability_365 IS NULL OR availability_365 < 0 THEN 1 ELSE 0 END) AS InvalidAvailability
FROM dbo.ProcessedTable
WHERE 
    price IS NULL OR price <= 0 OR 
    minimum_nights IS NULL OR minimum_nights < 0 OR 
    availability_365 IS NULL OR availability_365 < 0;