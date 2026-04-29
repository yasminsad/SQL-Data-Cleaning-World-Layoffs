/*
Data Cleaning Project: World Layoffs
Tools: MySQL Workbench
Skills: Staging Tables, CTEs, Window Functions, Self-Joins, Data Type Conversion
*/

-- -----------------------------------------------------------------------------------------------------------------------
-- 1. DATA STAGING
-- -----------------------------------------------------------------------------------------------------------------------

-- Create a staging table to work on. This keeps the raw data safe in case we make a mistake.
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT INTO layoffs_staging 
SELECT * FROM layoffs;

-- Create a second staging table to handle duplicate removal
-- This includes a 'row_num' column so we can easily delete the extras.
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;


-- -----------------------------------------------------------------------------------------------------------------------
-- 2. REMOVING DUPLICATES
-- -----------------------------------------------------------------------------------------------------------------------

-- We identified rows where row_num > 1 as duplicates.
DELETE FROM layoffs_staging2
WHERE row_num > 1;


-- -----------------------------------------------------------------------------------------------------------------------
-- 3. STANDARDIZING DATA
-- -----------------------------------------------------------------------------------------------------------------------

-- Trimming whitespace from company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardizing industry names (e.g., merging 'CryptoCurrency' and 'Crypto' into one category)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardizing country names (fixing trailing periods)
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Converting 'date' from text to DATE format
UPDATE layoffs_staging2
SET `date` = CASE
    WHEN `date` IS NULL OR `date` = '' THEN NULL
    ELSE STR_TO_DATE(`date`, '%Y-%m-%d')
END;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- -----------------------------------------------------------------------------------------------------------------------
-- 4. HANDLING NULLS AND BLANKS
-- -----------------------------------------------------------------------------------------------------------------------

-- Convert empty strings to NULLs so they are easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Use a self-join to fill in missing industry values based on other rows for the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Remove rows that are not useful (missing both key data points)
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- -----------------------------------------------------------------------------------------------------------------------
-- 5. FINAL CLEANUP
-- -----------------------------------------------------------------------------------------------------------------------

-- Drop the helper column used for duplicate removal
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final check of the data
SELECT * FROM layoffs_staging2;