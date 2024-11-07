-- Exploratary Data Analysis
-- Exploring the data, finding trend and pattern 


SELECT * 
FROM world_layoffs.layoffs_staging2;

-- go through the data and explore the information 

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were

SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- to check which company has percentage_laid_off = 1 (100%)

SELECT *
FROM world_layoffs.layoffs_staging2 
WHERE percentage_laid_off =1
ORDER BY total_laid_off DESC;

-- From the output, can noticed all these are mostly startups who all went out of business during this time

-- order by funds_raised_millions to see how big some of these companies were

SELECT *
FROM world_layoffs.layoffs_staging2 
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- BritishVolt has the highest amount of funds_raised_millions, about 2 billion dollars and went under, horrible 


-- to check on the companies with the biggest single Layoff
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2 
GROUP BY company
ORDER BY 2 DESC;

-- by location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC;

-- to check the date 

SELECT MIN(`date`), MAX(`date`)
FROM world_layoffs.layoffs_staging2;

-- the result showed from 2020 to 2023, total_laid_off might be starting from COVID period 

SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2 
GROUP BY industry
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2 
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2 
GROUP BY stage
ORDER BY 2 DESC;

-- to extract only month 

SELECT SUBSTRING(`date`,6,2) AS Month 
FROM world_layoffs.layoffs_staging2;


-- better to show month and year 

SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL 
GROUP BY `Month`
ORDER BY 1 ASC;

-- add up the total_laid_off month by month

WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL 
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_off, SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_Total;

-- we want to create a list, rank by each year with top 5 laid_off 

WITH Company_Year (Company, Years, Total_Laid_Off)AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
)
SELECT * 
FROM Company_Year;

-- after that we rank it and partition by year 

WITH Company_Year (Company, Years, Total_Laid_Off)AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
)
SELECT *, DENSE_RANK() OVER(partition by Years ORDER BY Total_laid_Off DESC) AS Ranking 
FROM Company_Year
WHERE Years IS NOT NULL
ORDER BY Ranking;

-- then, we want to group by year based on the ranking 

WITH Company_Year (Company, Years, Total_Laid_Off)AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
), Company_year_rank AS
(
SELECT *, DENSE_RANK() OVER(partition by Years ORDER BY Total_laid_Off DESC) AS Ranking 
FROM Company_Year
WHERE Years IS NOT NULL
ORDER BY Ranking
)
SELECT * 
FROM Company_year_rank
WHERE Ranking <=5;

