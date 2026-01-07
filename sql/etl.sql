// Vytvorenie databázy
CREATE DATABASE BroadBand_Markets;

// Extract
// Vytvorenie staging tabulky
CREATE OR REPLACE TABLE staging AS
SELECT * FROM EUROPEAN_BROADBAND_MARKETS_2017_GATOR_EAGLE.EBM.EUROPEAN_BROADBAND_MARKETS_2017_FREE_DATASET;

// Test
select * from staging;

// Load, Transform
// Vytvorenie tabulky dimenzie dim_region 
CREATE OR REPLACE TABLE dim_region AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY nuts3) AS id_region,
    nuts3,
    nuts3_name,
    CASE
        WHEN population < 50000 THEN 'Under 50K'
        WHEN population BETWEEN 50000 AND 100000 THEN '50K-100K'
        WHEN population BETWEEN 100000 AND 500000 THEN '100K-500K'
        WHEN population BETWEEN 500000 AND 1000000 THEN '500K-1M'
        WHEN population BETWEEN 1000000 AND 5000000 THEN '1M-5M'
        WHEN population BETWEEN 5000000 AND 10000000 THEN '5M-10M'
        WHEN population BETWEEN 10000000 AND 50000000 THEN '10M-50M'
        WHEN population > 50000000 THEN '50M+'
        ELSE 'Unknown'
    END AS population_group, // Zaradenie do skupiny podla počtu obyvateĽov
    CASE
        WHEN ROUND(overall_fixed_bb_percentage, 2) < 0.20 THEN 'Under 20%'
        WHEN ROUND(overall_fixed_bb_percentage, 2) BETWEEN 0.20 AND 0.39 THEN '20%-39%'
        WHEN ROUND(overall_fixed_bb_percentage, 2) BETWEEN 0.40 AND 0.59 THEN '40%-59%'
        WHEN ROUND(overall_fixed_bb_percentage, 2) BETWEEN 0.60 AND 0.79 THEN '60%-79%'
        WHEN ROUND(overall_fixed_bb_percentage, 2) BETWEEN 0.80 AND 0.89 THEN '80%-89%'
        WHEN ROUND(overall_fixed_bb_percentage, 2) BETWEEN 0.90 AND 0.99 THEN '90%-99%'
        WHEN ROUND(overall_fixed_bb_percentage, 2) > 0.99 THEN '100%'
        ELSE 'Unknown'
    END AS bb_coverage_group, // Zaradenie do skupiny podla % broadband pripojenia
    CASE
        WHEN ROUND(overall_nga_percentage, 2) < 0.20 THEN 'Under 20%'
        WHEN ROUND(overall_nga_percentage, 2) BETWEEN 0.20 AND 0.39 THEN '20%-39%'
        WHEN ROUND(overall_nga_percentage, 2) BETWEEN 0.40 AND 0.59 THEN '40%-59%'
        WHEN ROUND(overall_nga_percentage, 2) BETWEEN 0.60 AND 0.79 THEN '60%-79%'
        WHEN ROUND(overall_nga_percentage, 2) BETWEEN 0.80 AND 0.89 THEN '80%-89%'
        WHEN ROUND(overall_nga_percentage, 2) BETWEEN 0.90 AND 0.99 THEN '90%-99%'
        WHEN ROUND(overall_nga_percentage, 2) > 0.99 THEN '100%'
        ELSE 'Unknown'
    END AS nga_coverage_group, // Zaradenie do skupiny podla % nga pripojenia
FROM staging;

// Overenie
select * from dim_region;

// Vytvorenie tabulky dimenzie dim_country
CREATE OR REPLACE TABLE dim_country AS
SELECT
    ROW_NUMBER() OVER (ORDER BY country_code) AS id_country,
    country,
    country_code
FROM (SELECT DISTINCT country, country_code FROM staging);

SELECT DISTINCT country, country_code FROM staging; // Vytiahnutie len jedinečných krajín

// Overenie
SELECT * FROM dim_country;

// Vytvorenie tabulky dimenzie dim_date
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY reported_at) AS id_date,
    reported_at,
    YEAR(reported_at) AS year,
    MONTH(reported_at) AS month,
    DAY(reported_at) AS day
FROM (SELECT DISTINCT reported_at FROM staging);

// Overenie
SELECT * FROM dim_date;

-- SELECT s.country, s.country_code, c.id_country, c.country FROM staging s
-- JOIN dim_country c ON s.country_code = c.country_code;

-- SELECT c.id_country, c.country, s.* FROM staging s
-- JOIN dim_country c ON s.country_code = c.country_code;

// Vytvorenie tabulky faktov fact_broadband
CREATE OR REPLACE TABLE fact_broadband AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.nuts3) AS id_record,
    c.id_country,
    r.id_region,
    d.id_date,
    s.land_area,
    s.population,
    s.population_density,
    s.households,
    s.percentage_rural,
    s.rural_population,
    s.overall_fixed_bb_percentage,
    s.overall_fixed_bb_population,
    s.overall_nga_percentage,
    s.overall_nga_population,
    s.fttp_and_docsis31_percentage,
    s.fttp_and_docsis31_population,
    s.dsl_percentage,
    s.dsl_population,
    s.vdsl_percentage,
    s.vdsl_population,
    s.vdsl_2_vectoring_percentage,
    s.vdsl_2_vectoring_population,
    s.fttp_percentage,
    s.fttp_population,
    s.fwa_percentage,
    s.fwa_population,
    s.wimax_percentage,
    s.wimax_population,
    s.cable_percentage,
    s.cable_population,
    s.docsis_30_percentage,
    s.docsis_30_population,
    s.docsis_31_percentage,
    s.docsis_31_population,
    s.lte_percentage,
    s.lte_population,
    s.broadband_above_2mbps_percentage,
    s.broadband_above_2mbps_population,
    s.broadband_above_30mbps_percentage,
    s.broadband_above_30mbps_population,
    s.broadband_above_100mbps_percentage,
    s.broadband_above_100mbps_population
FROM staging s
JOIN dim_country c ON s.country_code = c.country_code
JOIN dim_region r ON s.nuts3 = r.nuts3
JOIN dim_date d ON s.reported_at = d.reported_at;

// Overenie
SELECT * FROM fact_broadband ORDER BY id_record;
SELECT * FROM fact_broadband ORDER BY population DESC;

// Vymazanie staging tabuľky
DROP TABLE IF EXISTS staging;