# European-Broadband-Markets-ETL
Implementácia ELT procesu v Snowflake a vytvorenie dátového skladu so schémou Star na datasete [European-Broadband-Markets-2017](https://app.snowflake.com/marketplace/listing/GZSVZ6EW2A/expert-intelligence-european-broadband-markets-2017?search=europe&pricing=free&available=available). Dataset má údaje z 1405 regiónov z 30 štátov. (Zameraním projektu je preskúmať pokrytie jednotlivých technológií prenosu dát na území Európy v roku 2017.) TOTO UPRAVIŤ!!

<hr>

## 1. Úvod a popis zdrojových dát
(Analyzujeme dáta o technológiach prenosu dát, ich pokrytí v jednotlivých štátoch a ich regiónov. Cieľ je:
- Zistiť prístupnosť prenosových technológií
- Identifikovať regióny) TOTO UPRAVIŤ!!

Dataset obsahuje jednu tabulku `EUROPEAN_BROADBAND_MARKETS_2017_FREE_DATASET`, ktorá obsahuje údáje o počte obyvateľov, domácností podľa regiónov a štátov a štatistiky o využívaní jednotlivých technológií (DSL, VDSL, FTTP...). 

Účelom ELT procesu je tieto dáta pripraviť, transformovať a sprístupniť na viacdimenzionálnu analýzu.

<hr>

### 1.1 ERD diagram zdrojového datasetu
Relačný model dát z tohoto datasetu je znázornený v entitno-relačnom diagrame:
<p align="center">
  <img src="img/erd-schema.png">
  <br>
  <em>Obrázok 1 - Entitno_relačná schéma</em>
</p>

<hr>

## 2. Dimenzionálny model
Návrh schémy hviezdy podľa Kimballovej metodológie, obsahuje 1 tabuľku faktov `fact_broadband`, ktorá je prepojená s nasledujúcimi 3 dimenziami:
- `dim_region` ukladá názov regiónu, jeho skratku a kategórie do ktorých spadá. SCD typ 1
- `dim_country` ukladá krajinu a jej skratku. SCD typ 1
- `dim_date` obsahuje dátum merania štatistík. SCD typ 0

Tabulka faktov `fact_broadband` obsahuje PK `id_record`, FK `id_region, id_country, id_date` a ďalej údaje o ploche kraja, počte obyvateľov, hustoty obyvateľstva, počtu domácností a ďalšie údaje zamerané na počet obyvateľstva využívajúceho danú technológiu a jej percentuálna časť s celkového obyvateľstva. 

Schéma hviezdy je znázornená na diagrame pre vizualizáciu prepojenia medzi tabulkami dimenzií a tabulky faktov:
<p align="center">
  <img src="img/star-schema.png">
  <br>
  <em>Obrázok 2 - Star schéma pre European-Broadband-Markets-2017</em>
</p>

<hr>

## 3. ELT proces v Snowflake
**ELT** je proces rozdelený na tri časti: **Extract, Load, Transform**.  
Tento proces nám v Snowflake-u pomôže zdrojové údaje pripraviť zo staging vrstvy do dimenzionálneho modelu, ktorý je vhodný na analýzu a vizualizáciu.

<hr>

### 3.1 Extract
Zdrojový dataset sme si stiahli do spoločného priestoru, vďaka tomu sme dáta priamo nahrali do staging tabuľky a nepotrebovali sme manuálne sťahovať žiadne súbory. Staging tabuľka slúži na dočasné uloženie importovaných dát. 

**Zdrojová databáza:** [European-Broadband-Markets-2017](https://app.snowflake.com/marketplace/listing/GZSVZ6EW2A/expert-intelligence-european-broadband-markets-2017?search=europe&pricing=free&available=available)  
**Schéma:** EBM  
**SQL kód:**
```sql
CREATE OR REPLACE TABLE staging AS
SELECT * FROM EUROPEAN_BROADBAND_MARKETS_2017_GATOR_EAGLE.EBM.EUROPEAN_BROADBAND_MARKETS_2017_FREE_DATASET;

// Test
select * from staging;
```

<hr>

### 3.2 Load
Následne sme načítavali dáta do jednotlivých tabuliek podľa navrhnutého dimenzionálneho modelu. Na to sme použili pri vytváraní dimenzie príkaz `AS SELECT`, ktorý nam umožnil kopírovať dáta priamo z `staging` tabuľky. Každej tabulke sme pomocou `ROW_NUMBER()` funkcie vytvorili primárne klúče. Po načítaní údajov do tabuliek sme overili správnosť načítania. Príklad je z tabuľky faktov:

**SQL kód:**
```sql
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
```

<hr>

### 3.3 Transform
Pri transformovaní dát bolo potrebné využiť SQL funkcie na čistenie, úpravu a obohatenie pôvodných dát:
- Deduplikovať názvy krajín pomocou `SELECT DISTINCT`
- Vytvoriť primárne klúče cez window function `ROW_NUMBER()`
- Rozdeliť dátum cez funkcie `YEAR()`, `MONTH()`, `DAY()`
- Pridať kategórie cez `CASE`

Cieľom bolo získať dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú analýzu.

Dimenzie sme navrhli tak aby poskytovali dáta pre tabuľku faktov. `dim_region` má údaje o názve regiónu, skratke daného regiónu a kategórie do ktorých spadá podľa počtu obyvateľov (napr. *"500K-1M"*), percenta obyvateľov s širokopásmovým (broadband) pripojením a percenta obyvateľov s Next Generation Access (NGA) prístupom (napr. *"60-79%"*). Táto dimenzia je typu `SCD 1`, teda neuchováva históriu názvov ani kategórií.

**SQL kód:**
```sql
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
SELECT * FROM dim_country;
```
Dimenzia `dim_date` uchováva údaje o dátumoch, v ktorých sa uskutočnil zber štatistík. Má ďalej rozdelené údaje ako deň, mesiac a rok. Táto dimenzia umožňuje porovnávať zmeny v pokrytí postupom rokov alebo v rámci roka. Táto dimenzia má typ `SCD 0` pretože záznamy v tejto dimenzii sú nemenné.

**SQL kód:**
```sql
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
```

Posledná dimenzia `dim_country` obsahuje názov a kód krajiny. Tiež je typu `SCD 1` ako `dim_region`, pretože nepotrebujeme uchovávať históriu názvov krajín alebo ich kódov.  

**SQL kód:**
```sql
CREATE OR REPLACE TABLE dim_country AS
SELECT
    ROW_NUMBER() OVER (ORDER BY country_code) AS id_country,
    country,
    country_code
FROM (SELECT DISTINCT country, country_code FROM staging);

SELECT DISTINCT country, country_code FROM staging; // Vytiahnutie len jedinečných krajín

// Overenie
SELECT * FROM dim_country;
```

Po dokončení ETL procesu môžeme uvoľniť priestor odstránením zdrojových dát zo staging tabuľky pomocou príkazu:

**SQL kód:**
```sql
// Vymazanie staging tabuľky
DROP TABLE IF EXISTS staging;
```

<hr>

## 4. Vizualizácia dát

<hr>

**Autori:** [Matúš Gabaš](https://github.com/snickers-c) a [Juraj Daniš](https://github.com/Jur1n0)
