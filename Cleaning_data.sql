-- Data Cleaning

select 
	*
from 
	layoffs;

-- Remove Duplicates
-- Create a staging table `layoffs_staging` with the same structure as `layoffs`
CREATE TABLE 
    layoffs_staging
LIKE 
    layoffs;

-- Copy all data from `layoffs` to `layoffs_staging`
INSERT 
    layoffs_staging
SELECT *
FROM 
    layoffs;

-- Select data from `layoffs_staging` with an added `duplicates_row` column
-- `duplicates_row` identifies duplicates based on specified columns (e.g., company, location)
SELECT 
    *,
    ROW_NUMBER() 
        OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS duplicates_row
FROM 
    layoffs_staging;

-- Create a new table `layoffs_staging2` with all original columns plus `duplicates_row` to mark duplicates
CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `duplicates_row` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Populate `layoffs_staging2` with data from `layoffs_staging`, adding `duplicates_row` column
INSERT INTO layoffs_staging2
SELECT 
    *,
    ROW_NUMBER() 
        OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS duplicates_row
FROM 
    layoffs_staging;

-- Delete duplicate records in `layoffs_staging2`, keeping only the first instance in each duplicate set
DELETE
FROM 
	layoffs_staging2
WHERE 
	duplicates_row > 1;

-- Standardizing The Data

-- Select company names and their trimmed versions from `layoffs_staging2`
SELECT 
    company, 
    TRIM(company)
FROM 
    layoffs_staging2;

-- Remove leading and trailing spaces from the `company` column in `layoffs_staging2`
UPDATE 
    layoffs_staging2
SET 
    company = TRIM(company);

-- Select distinct values from the `industry` column in `layoffs_staging2`
SELECT 
    DISTINCT industry
FROM 
    layoffs_staging2;

-- Standardize the `industry` column to 'Crypto' for entries that start with 'Crypto'
UPDATE 
    layoffs_staging2
SET 
    industry = 'Crypto'
WHERE 
    industry LIKE 'Crypto%';

-- Select distinct country names from `layoffs_staging2` and sort them alphabetically
SELECT 
    DISTINCT country
FROM 
    layoffs_staging2
ORDER BY 
    1;

-- Remove trailing period in `country` names that start with 'United States'
UPDATE 
    layoffs_staging2
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE 
    country LIKE 'United States%';

-- Select all values from the `date` column in `layoffs_staging2`
SELECT
    `date`
FROM
    layoffs_staging2;

-- Convert the `date` column format to 'YYYY-MM-DD' format
UPDATE
    layoffs_staging2
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter the `date` column type to DATE for consistent storage of dates
ALTER TABLE 
	layoffs_staging2
MODIFY COLUMN 
	`date` DATE;



-- Null/Blank values


-- Select all records where both 'total_laid_off' and 'percentage_laid_off' fields are NULL.
-- This query identifies entries that might be missing important data on layoffs.
SELECT 
	*
FROM 
	layoffs_staging2
WHERE
	total_laid_off IS NULL AND
    percentage_laid_off IS NULL;


-- Update the 'industry' field to NULL where it's currently an empty string ('').
-- This standardizes the data by replacing empty fields with NULL values for consistency.
UPDATE 
	layoffs_staging2
SET 
	industry = NULL
WHERE 
	industry = '';


-- Select all records where 'industry' is either NULL or an empty string.
-- This helps to locate and review entries with missing or incomplete 'industry' data.
SELECT 
	*
FROM
	layoffs_staging2
WHERE
	industry IS NULL OR
    industry = '';


-- Join the table 'layoffs_staging2' with itself to find records with the same company name.
-- Select records where 'industry' is NULL in one record and not NULL in the other.
-- This identifies cases where missing 'industry' information can be filled in based on other records of the same company.
SELECT
	*
FROM 
	layoffs_staging2 t1
JOIN
	layoffs_staging2 t2
ON
	t1.company = t2.company
WHERE
	t1.industry IS NULL AND
    t2.industry IS NOT NULL;


-- Update the 'industry' field in records with NULL values by joining with records of the same company.
-- The non-NULL 'industry' value from 't2' is used to populate the NULL 'industry' field in 't1' where possible.
UPDATE 
	layoffs_staging2 t1
JOIN 
	layoffs_staging2 t2
ON
	t1.company = t2.company
SET 
	t1.industry = t2.industry
WHERE 
	t1.industry IS NULL AND
    t2.industry IS NOT NULL;


	
-- Remove Any unnecessary columns/rows

-- Select all records where both 'total_laid_off' and 'percentage_laid_off' fields are NULL.
-- This helps identify entries that might be missing important data on layoffs.
SELECT 
	*
FROM 
	layoffs_staging2
WHERE
	total_laid_off IS NULL AND
    percentage_laid_off IS NULL;
    

-- Delete records where both 'total_laid_off' and 'percentage_laid_off' are NULL.
-- This removes entries that lack key information on layoffs, ensuring data quality.
DELETE	
FROM 
	layoffs_staging2
WHERE
	total_laid_off IS NULL AND
    percentage_laid_off IS NULL;


-- Select all remaining records from 'layoffs_staging2' after deletions.
-- This shows the current state of the table, allowing you to verify the deletions.
SELECT 
	*
FROM
	layoffs_staging2;
    

-- Alter the table 'layoffs_staging2' to drop the 'duplicates_row' column.
-- This cleans up the schema by removing a temporary column used for identifying duplicates.
ALTER TABLE 
	layoffs_staging2
DROP COLUMN 
	duplicates_row;


    