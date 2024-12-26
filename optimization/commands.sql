--Create brin index for date 
CREATE INDEX idx_date_brin ON "airMeasurement" USING BRIN (date);
-- Create index for pair city_ID and chemical code
CREATE INDEX idx_city_chemical ON "airMeasurement" ("city_ID", "chemicalCode");
--Create materialized view for monthly measurement aggregates
CREATE MATERIALIZED VIEW public."monthlyAirMeasurement" AS
SELECT
    DATE_TRUNC('month', date) AS month,
    "city_ID",
    "chemicalCode",
    "measureUnitCode",
    AVG(value) AS average_value
FROM
    public."airMeasurement"
GROUP BY
    DATE_TRUNC('month', date),
    "city_ID",
    "chemicalCode",
    "measureUnitCode";
