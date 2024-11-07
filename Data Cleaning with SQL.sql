-- SQL Project - Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- 1. Remove Duplicates 
-- 2. Standardize the Data 
-- 3. Null Values or blank values 
-- 4. Remove any columns 

SELECT * 
FROM world_layoffs.layoffs;

-- Create a staging table for data cleaning and keep an original copy of dataset 

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- 1. Remove Duplicates
# First check for duplicates


SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- row_num > 1 are all duplicates 

-- Let's look at Oda to confirm the above observation 

SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;

-- From the output, it looks like these are all legitimate entries and shouldn't be deleted. Hence, we need to really look at every single row to be accurate

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Now, the output are all the real duplicates that we want to remove from the dataset where row numberis > 1 or 2 or greater essentially

-- In order to delete those rows, write the queries as below: 

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT* 
FROM duplicate_cte
WHERE row_num >1;

-- A good way to delete is to create a new column for the duplicates and remove from that column 
-- Method: right click layoffs_staging > copy to clipboard > create statement > paste 

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM world_layoffs.layoffs_staging2;

INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM world_layoffs.layoffs_staging2;

-- 2. Standardize the data 

-- to remove space 
SELECT company, TRIM(company) 
FROM world_layoffs.layoffs_staging2;

-- to change the original company to TRIM(company) in dataset 
UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

-- if looking at industry column, it seems like we have some null and empty rows, check on this 
SELECT DISTINCT(industry) 
FROM world_layoffs.layoffs_staging2
ORDER BY 1; 

-- notice that Crypto has multiple different variations. We need to standardize that.
-- standardize the naming of Crypto 

SELECT * 
FROM world_layoffs.layoffs_staging2 
WHERE industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(industry)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- continue to check on each column one by one 

-- after checking country column, notice that there is one row with "United States.", lets's standardize this to "United States" 

SELECT DISTINCT(country)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(country), TRIM(TRAILING'.' FROM country)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(Trailing '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT(country)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- to fix the date column, convert it to date format (from TEXT to DATE) 

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM world_layoffs.layoffs_staging2;

-- use str-to-date to update this field 

UPDATE world_layoffs.layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

SELECT `date`
FROM world_layoffs.layoffs_staging2;

-- now, convert the data type properly

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Remove NULL and blank values 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. 
-- no changing needed, will keep them null because it makes it easier for calculations during the EDA phase

-- continue to check other column 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- try to populate the result with NULL / blank values
-- for example, using company = Airbnb to check 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb';

-- the output showed that Airbnb industry is 'Travel', but this just isn't populated
-- continue to populate the other 3 companys to check 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Carvana';

-- the result showed that Carvana industry is 'Transportation', then we can fill up the blank value 


-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- check on location as well cause it might have same company name but in different location 
-- makes it easy so if there were thousands we wouldn't have to manually check them all


SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- after running this query, noticed that there is 0 row updated, which mean still the same, need to find out why 
-- it might probably due to t1.industry are all blank value, so let us set t1.industry which is blank to NULL 

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';



-- check back Airbnb again and can notice industry column with blank value already fill up with 'Travel' 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb';

-- check back and noticed Bally's was the only one without a populated row to populate this null values

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- 4. Remove any columns or rows 
-- deleting data that we are not using (which is blank) 

DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- drop the row_num that created before 

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num; 

-- finally, check throught the dataset again to finalise 

SELECT * 
FROM world_layoffs.layoffs_staging2;