// Graf 1: Pokrytie internetom >100 Mbps (%) podľa krajín.
USE DATABASE BROADBAND_MARKETS;
USE SCHEMA BROADBAND_MARKETS.PUBLIC;

SELECT 
    d.country,
    TO_VARCHAR(ROUND(AVG(f.broadband_above_100mbps_percentage) * 100, 2)) || '%' AS avg_percentage
FROM dim_country AS d
JOIN fact_broadband AS f
    ON d.id_country = f.id_country
GROUP BY d.country
ORDER BY AVG(f.broadband_above_100mbps_percentage) DESC;

//Graf 2: Pokrytie NGA v slovenských krajoch.
USE DATABASE BROADBAND_MARKETS;
USE SCHEMA BROADBAND_MARKETS.PUBLIC;

SELECT 
    r.NUTS3_NAME, 
    f.OVERALL_NGA_POPULATION
FROM PUBLIC.DIM_REGION r
JOIN PUBLIC.FACT_BROADBAND f ON r.ID_REGION = f.ID_REGION
WHERE r.NUTS3_NAME != 'Slovakia'
  AND r.NUTS3 LIKE 'SK0%'
ORDER BY f.OVERALL_NGA_POPULATION DESC;

//Graf 3: Zobrazenie penetrácie technológií v Európe.
USE DATABASE BROADBAND_MARKETS;
USE SCHEMA BROADBAND_MARKETS.PUBLIC;

SELECT
    d.country,
    ROUND(AVG(f.dsl_percentage) * 100, 2) AS dsl_pct,
    ROUND(AVG(f.fttp_percentage) * 100, 2) AS fttp_pct,
    ROUND(AVG(f.cable_percentage) * 100, 2) AS cable_pct
FROM fact_broadband f
JOIN dim_country d
    ON f.id_country = d.id_country
GROUP BY d.country
ORDER BY fttp_pct DESC;

//Graf 4: Bodový graf korelácie hustoty a rýchlosti internetu.

USE DATABASE BROADBAND_MARKETS;
USE SCHEMA BROADBAND_MARKETS.PUBLIC;

SELECT
    d.country,
    AVG(f.population_density) AS population_density,
    ROUND(AVG(f.broadband_above_100mbps_percentage) * 100, 2) AS broadband_100mbps_pct
FROM fact_broadband f
JOIN dim_country d
    ON f.id_country = d.id_country
GROUP BY d.country;

//Graf 5: Top 20 regiónov s najlepším pokrytím internetu >100 Mbps.

USE DATABASE BROADBAND_MARKETS;
USE SCHEMA BROADBAND_MARKETS.PUBLIC;

SELECT 
    r.nuts3_name || ' (' || c.country || ')' AS region_location,
    TO_VARCHAR(ROUND(MAX(f.broadband_above_100mbps_percentage) * 100, 2)) || '%' AS coverage_100mbps_pct
FROM fact_broadband f
JOIN dim_region r 
    ON f.id_region = r.id_region
JOIN dim_country c 
    ON f.id_country = c.id_country
WHERE r.nuts3 NOT LIKE '%-TOTAL%'
GROUP BY 1
ORDER BY coverage_100mbps_pct DESC
LIMIT 20;

//Graf 6: Top 20 regiónov s najväčším kladným rozdielom oproti priemeru ich krajiny.

USE DATABASE BROADBAND_MARKETS;
USE SCHEMA BROADBAND_MARKETS.PUBLIC;

WITH RegionalBase AS (
    SELECT 
        r.nuts3_name || ' (' || c.country || ')' AS region_label,
        c.country,
        f.overall_nga_percentage * 100 AS nga_pct
    FROM fact_broadband f
    JOIN dim_region r 
        ON f.id_region = r.id_region
    JOIN dim_country c 
        ON f.id_country = c.id_country
    WHERE r.nuts3_name NOT LIKE '%-TOTAL%' 
)
SELECT 
    region_label,
    ROUND(nga_pct, 2) AS region_nga_pct,
    ROUND(AVG(nga_pct) OVER(PARTITION BY country), 2) AS national_avg_pct,
    ROUND(nga_pct - AVG(nga_pct) OVER(PARTITION BY country), 2) AS deviation_pct
FROM RegionalBase
ORDER BY deviation_pct DESC
LIMIT 20;