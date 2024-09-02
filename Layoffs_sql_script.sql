/*
Data cleaning

Goals
 a. remove duplicates
 b. standardize the data
 c. deal with null values or blank values if applicable
 d. remove any that will not be used columns 
*/

/*creating a table that will be worked on containing the same raw data as layoffs table
NOTE; layoffs is not worked on, it is backup in the event that erros are made when 
cleaning the data
*/

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

/*
identify duplicates
*/
  
WITH duplicate_cte AS
(
SELECT 
  *, 
  ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, 
    percentage_laid_off, 'Date', stage, country, funds_raised_millions
  ) as row_num 
From 
  layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

/*creating layoffs_staging2 table to insert data from layoffs_staging table
to delete the identied dubliplates, as DELETE can not be used on the
 duplicate_cte or layoffs_staging table as this would delete all the duplicating rows
*/

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT 
  *, 
  ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, 
    percentage_laid_off, 'Date', stage, country, funds_raised_millions
  ) as row_num 
From 
  layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

/*
 Standardizing data (finding issues with the data and fixing it)
*/

/*
removing white space at both ends of company names
*/

SELECT 
    company, TRIM(company)
FROM
    layoffs_staging2;
    
UPDATE layoffs_staging2
SET company = TRIM(company);

-- updating differently written industry names refering to the same industry

SELECT DISTINCT(industry)
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

/*
updating names of countries written incorrectly
*/

SELECT DISTINCT(country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET  country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

/*
 updating Date column's data type and format
 */

SELECT `date` 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

/*
updating and populate nulls and blanks in the data if liable
*/

/* rows with null values in both total_laid_off and percentage_laid_off
columns are of no value to the data set as it is unknown if there were any layoffs done
*/

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;


/*
 droping columns that will not be useful in the analysis
 */
 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;







