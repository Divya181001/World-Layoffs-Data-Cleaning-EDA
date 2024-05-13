CREATE SCHEMA world_layoffs;

-- ----------------------------------------- << DATA CLEANING >> ------------------------------------------------------------

SELECT 
    *
FROM
    world_layoffs.layoffs;

CREATE TABLE layoffs_staging LIKE world_layoffs.layoffs;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging;

INSERT layoffs_staging SELECT * FROM world_layoffs.layoffs;

-- 1)  Remove the duplicates 

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT * FROM
duplicate_cte
WHERE row_num > 1;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT * FROM
duplicate_cte
WHERE row_num > 1;

-- To delete the duplicate entries, creating another copy of table layoffs_staging and adding the row_num column into it 

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
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;



INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging ;

-- Rechecking the duplicate entries before deleting them from the table

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    row_num > 1;

-- Deleting the duplicate entries

DELETE FROM world_layoffs.layoffs_staging2 
WHERE
    row_num > 1;

-- 2)  Standardizing the data

SELECT 
    company, TRIM(company)
FROM
    world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2 
SET 
    company = TRIM(company);

-- Checking for inconsistencies in the industry column

SELECT DISTINCT
    industry
FROM
    world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging2 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE 'Crypto%';

-- Checking for inconsistencies in the country column

SELECT DISTINCT
    country
FROM
    world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM country)
FROM
    world_layoffs.layoffs_staging2
ORDER BY 1;

UPDATE world_layoffs.layoffs_staging2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'United States%';

-- standardizing the date column 

SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE world_layoffs.layoffs_staging2 MODIFY COLUMN `date` DATE;


-- 3) Checking for null values

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

UPDATE world_layoffs.layoffs_staging2 
SET 
    industry = NULL
WHERE
    industry = ''; -- updating to null for an easier way of identifying and replacing values 

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    industry IS NULL OR industry = '';

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    company = 'Airbnb';

-- Updating the blank values few companies in the industry column

SELECT 
    t1.industry, t2.industry
FROM
    world_layoffs.layoffs_staging2 t1
        JOIN
    world_layoffs.layoffs_staging2 t2 ON t1.company = t2.company
        AND t1.location = t2.location
WHERE
    (t1.industry IS NULL OR t1.industry = '')
        AND t2.industry IS NOT NULL;
        
UPDATE world_layoffs.layoffs_staging2 t1
        JOIN
    world_layoffs.layoffs_staging2 t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;
 
-- 4) Removing any columns or rows which are not required for EDA

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

DELETE FROM world_layoffs.layoffs_staging2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

-- Removing the column row_num which is not needed anymore

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2 DROP COLUMN row_num;

-- ---------------------------------------- << EXPLORATORY DATA ANALYSIS >> --------------------------------------------------

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2;

SELECT 
    MAX(total_laid_off), MAX(percentage_laid_off)
FROM
    world_layoffs.layoffs_staging2;

-- 1) What are the companies that had a 100 percentage layoff 

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- 2) What are the companies with the biggest single Layoff

SELECT 
    company, SUM(total_laid_off)
FROM
    world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- 3) The day at which the layoffs started and the last date as of now where the layoffs happened

SELECT 
    MIN(`date`), MAX(`date`)
FROM
    world_layoffs.layoffs_staging2;

-- 4) Which industries had the most number of layoffs

SELECT 
    industry, SUM(total_laid_off)
FROM
    world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- 5) Which country had the most number of layoffs

SELECT 
    country, SUM(total_laid_off)
FROM
    world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- 6) Which year had the highest layoffs 

SELECT 
    YEAR(`date`), SUM(total_laid_off)
FROM
    world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

SELECT 
    stage, SUM(total_laid_off)
FROM
    world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- 7) Rolling Total of Layoffs Per Month 

SELECT 
    SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM
    world_layoffs.layoffs_staging2
WHERE
    SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_layoffs, SUM(total_layoffs) OVER(ORDER BY `MONTH`) AS rolling_total 
FROM Rolling_total ;

-- 8) Companies with the most layoffs per year

SELECT 
    company, YEAR(`date`), SUM(total_laid_off)
FROM
    world_layoffs.layoffs_staging2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
), 
Company_year_rank AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_year
WHERE years IS NOT NULL
)
SELECT * FROM Company_year_rank 
WHERE Ranking <= 5 ;











