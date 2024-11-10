-- Covid 19 Data Exploration 

-- Skills used: Joins, Converting Data Types, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views


SELECT * 
FROM portfolioproject.coviddeaths
WHERE continent IS NOT NULL
ORDER BY 3,4
;

-- Change the definition of date from text to datetime by creating a new column for both table (coviddeaths and covidvaccinations)

-- Change for coviddeaths table 
ALTER TABLE portfolioproject.coviddeaths ADD COLUMN date_converted DATETIME
;


UPDATE portfolioproject.coviddeaths 
SET date_converted = STR_TO_DATE(date, '%d/%m/%Y %H:%i:%s')
;

-- Change for covidvaccinations table 

ALTER TABLE portfolioproject.covidvaccinations ADD COLUMN date_converted DATETIME
;


UPDATE portfolioproject.covidvaccinations 
SET date_converted = STR_TO_DATE(date, '%d/%m/%Y %H:%i:%s')
;

-- Select Data that we are going to be starting with

SELECT Location, date_converted, total_cases, new_cases, total_deaths, population
FROM portfolioproject.coviddeaths
WHERE continent IS NOT NULL 
ORDER BY 1,2
;

-- Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

SELECT Location, date_converted, total_cases,total_deaths, (total_deaths/total_cases)*100 AS DeathPercentage
FROM portfolioproject.coviddeaths
WHERE location LIKE '%Malaysia%'
AND continent IS NOT NULL 
ORDER BY 1,2
;

-- Total Cases vs Population
-- Shows what percentage of population infected with Covid

SELECT Location, date_converted, Population, total_cases,  (total_cases/population)*100 AS PercentPopulationInfected
FROM portfolioproject.coviddeaths
ORDER BY 1,2
;

-- Countries with Highest Infection Rate compared to Population

SELECT Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 AS PercentPopulationInfected
FROM portfolioproject.coviddeaths
GROUP BY Location, Population
ORDER BY PercentPopulationInfected DESC
;

-- Countries with Highest Death Count per Population

SELECT Location, MAX(cast(Total_deaths as Signed)) AS TotalDeathCount
FROM portfolioproject.coviddeaths
WHERE continent IS NOT NULL 
GROUP BY Location
ORDER BY TotalDeathCount DESC
;

-- BREAKING THINGS DOWN BY CONTINENT

-- Showing continents with the highest death count per population

SELECT continent, MAX(cast(Total_deaths as Signed)) AS TotalDeathCount
FROM portfolioproject.coviddeaths
WHERE continent IS NOT NULL AND continent != ''
GROUP BY continent
ORDER BY TotalDeathCount DESC
;


-- GLOBAL NUMBERS

SELECT SUM(new_cases) AS total_cases, SUM(cast(new_deaths AS Signed)) AS total_deaths, SUM(cast(new_deaths AS Signed))/SUM(New_Cases)*100 AS DeathPercentage
FROM portfolioproject.coviddeaths
WHERE continent IS NOT NULL AND continent != ''
ORDER BY 1,2
;

-- Total Population vs Vaccinations
-- Shows Percentage of Population that has received at least one Covid Vaccine

SELECT dea.continent, dea.location, dea.date_converted, dea.population, vac.new_vaccinations
, SUM(CONVERT(vac.new_vaccinations, Signed)) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.date_converted) AS RollingPeopleVaccinated
FROM portfolioproject.coviddeaths dea
JOIN portfolioproject.covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date_converted = vac.date_converted 
WHERE dea.continent IS NOT NULL  AND dea.continent != ''
ORDER BY 2,3
;


-- Using CTE to perform Calculation on Partition By in previous query

WITH PopvsVac (Continent, Location, date_converted, Population, New_Vaccinations, RollingPeopleVaccinated)
AS
(
SELECT dea.continent, dea.location, dea.date_converted, dea.population, vac.new_vaccinations
, SUM(CONVERT(vac.new_vaccinations, Signed)) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.date_converted) AS RollingPeopleVaccinated
FROM portfolioproject.coviddeaths dea
JOIN portfolioproject.covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date_converted = vac.date_converted
WHERE dea.continent IS NOT NULL  AND dea.continent != ''
)
SELECT *, (RollingPeopleVaccinated/Population)*100
FROM PopvsVac
;


-- Using Temp Table to perform Calculation on Partition By in previous query

DROP TEMPORARY TABLE IF EXISTS PercentofPopulationVaccinated;

CREATE TEMPORARY TABLE PercentofPopulationVaccinated
(
    Continent VARCHAR(255),
    Location VARCHAR(255),
    Date_converted DATETIME,
    Population DECIMAL,
    New_vaccinations DECIMAL,
    RollingPeopleVaccinated DECIMAL
);

-- Insert data with SUM window function and TRIM/NULLIF for handling spaces or empty strings
-- TRIM(vac.new_vaccinations): This removes any leading or trailing spaces from the new_vaccinations value.
-- NULLIF(TRIM(vac.new_vaccinations), ''): This treats empty strings as NULL, which helps avoid errors in CAST.
-- COALESCE(..., 0): This replaces any resulting NULL values with 0.
-- These adjustments should handle cases where vac.new_vaccinations might contain empty strings or non-numeric values interpreted as 0, allowing CAST() to work without error.

INSERT INTO PercentofPopulationVaccinated
SELECT dea.continent, dea.location, dea.date_converted, dea.population, 
    COALESCE(CAST(NULLIF(TRIM(vac.new_vaccinations), '') AS DECIMAL), 0) AS New_vaccinations,
    SUM(COALESCE(CAST(NULLIF(TRIM(vac.new_vaccinations), '') AS DECIMAL), 0)) OVER (PARTITION BY dea.location ORDER BY dea.date_converted) AS RollingPeopleVaccinated
FROM 
    portfolioproject.coviddeaths dea
JOIN 
    portfolioproject.covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date_converted = vac.date_converted
;


-- Select with calculation
SELECT *, (RollingPeopleVaccinated / Population) * 100 AS PercentPopulationVaccinated
FROM PercentofPopulationVaccinated;


-- Creating View to store data for later visualizations in Tableau 

CREATE VIEW PercentofPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date_converted, dea.population, 
    COALESCE(CAST(NULLIF(TRIM(vac.new_vaccinations), '') AS DECIMAL), 0) AS New_vaccinations,
    SUM(COALESCE(CAST(NULLIF(TRIM(vac.new_vaccinations), '') AS DECIMAL), 0)) OVER (PARTITION BY dea.location ORDER BY dea.date_converted) AS RollingPeopleVaccinated
FROM 
    portfolioproject.coviddeaths dea
JOIN 
    portfolioproject.covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date_converted = vac.date_converted
WHERE dea.continent IS NOT NULL AND dea.continent != ''
;


