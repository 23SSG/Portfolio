-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or Blank Values
-- 4. Remove Any Columns

-- First create a copy of the raw data and work on the copy one

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;



SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off,'date') AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num >1 ;

SELECT *
FROM layoffs_staging
WHERE company ='Elemy';
-- Checking with company showed multiple values which are not duplicates, hence need to correct the partition statement and add all columns to it

-- As we cannot delete duplicate values in CTE directly (row_num >1), we need to create a new table and then remove those values having 
-- row number greater than two.We use the copy to clipboard create statment and create a new table wit row number as added column



CREATE TABLE layoffs_staging_2 (
  company text,
  location text,
  industry text,
  total_laid_off int DEFAULT NULL,
  percentage_laid_off text,
  date text,
  stage text,
  country text,
  funds_raised_millions int DEFAULT NULL,
  row_num INT
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  
  
SELECT *
FROM layoffs_staging_2;

INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Now looking for duplicates in the new created table layoffs_staging_2

SELECT *
FROM  layoffs_staging_2
WHERE row_num > 1;


-- Deleting the duplicates

SET SQL_SAFE_UPDATES = 0;
DELETE
FROM  layoffs_staging_2
WHERE row_num <> 1;
SET SQL_SAFE_UPDATES = 1;

-- check if the rows are removed

SELECT *
FROM  layoffs_staging_2
WHERE row_num > 1;

SELECT *
FROM  layoffs_staging_2;

ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;

-- Standardizing data
-- 1. check company
SELECT company,(TRIM(company))
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET company=TRIM(company);



-- 2. check industry
-- check distinct industries and figure out which can be grouped together

SELECT  DISTINCT industry
FROM layoffs_staging_2
order by 1;

-- Looks like the crypto and crypto currency can be grouped 

SELECT *
FROM layoffs_staging_2
WHERE industry LIKE 'Crypto%';

-- Check if its updated
SELECT DISTINCT industry
FROM layoffs_staging_2;



-- Need to update all cryptocurrency to crypto

UPDATE layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 3. check location

SELECT DISTINCT location
FROM layoffs_staging_2
order by 1;

-- 4. check for country

SELECT DISTINCT country
FROM layoffs_staging_2
order by 1;

-- Found two formats of united states and that needs to be corrected

SELECT  country
FROM layoffs_staging_2
WHERE country LIKE 'United States';

SELECT  DISTINCT country, TRIM(TRAILING '.' FROM country) 
FROM layoffs_staging_2
ORDER BY 1;

UPDATE layoffs_staging_2
SET country =TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- check if update for country is done
SELECT  DISTINCT country
FROM layoffs_staging_2
order by 1;

-- 5. check for date, very important if we want to time series visulaizations later on we need to change the format of date which is text to a date

SELECT  date
FROM layoffs_staging_2;


-- date in string format converted to month/day/year format

SELECT  date,
STR_TO_DATE(date,'%m/%d/%Y' )
FROM layoffs_staging_2;

-- Updating the date column in the data

UPDATE layoffs_staging_2
SET date  = STR_TO_DATE(date,'%m/%d/%Y');

-- check if the update for the date is done

SELECT  date
FROM layoffs_staging_2;

-- Alter inside the table the date column to be a DATE format

ALTER TABLE layoffs_staging_2
MODIFY column date DATE;


-- Now to we need to remove the blank and the null values

-- check null values for total_laid_off and percentage_laid_off and save the query to take further action on it

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Also need to check null value in column industry

SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL;

SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL
OR industry = ' ';

-- query showed three rows of Bally's Interactive with null values for industry type.
-- Also these are two duplicate rows with same information for Bally's Interactive which need to be removed
-- Further check values for company Bally's Interactive
-- SQL showed errors when query was company = 'Bally's Interactive' and used the other way around to check Bally's Interactive
SELECT *
FROM layoffs_staging_2
WHERE company LIKE 'Bally%';

-- check with keyword search interactive to look for industry type

SELECT *
FROM layoffs_staging_2
WHERE company LIKE '%Interactive%';

-- query shows Pico Interactive as "Other" for industry type and same location, hence we can populate the 
-- null values in the Bally's Interactive as "Other" as the industry type that has same location
-- let's use join to fill industry with null values to the values which are populated in industry type

SELECT *
FROM layoffs_staging_2 t1
JOIN layoffs_staging_2 t2
     ON t1.company = t2.company
	 AND t2.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry =' ')
AND t2.industry IS NOT NULL; 

-- could not find anything in the join
-- hence setting up values for Bally's Interactive to "other"

UPDATE layoffs_staging_2
SET industry = 'Other'
WHERE industry IS NULL;
 

    

-- CHECK FOR UPDATE
SELECT *
FROM layoffs_staging_2
WHERE industry like 'interactive%';

-- Deleting data with total_laid_off and percentage_laid_off with null values as we cannot populate them
-- plus they cannot be used for exploratory analysis 

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- check to see if the data is deleted

SELECT *
FROM layoffs_staging_2