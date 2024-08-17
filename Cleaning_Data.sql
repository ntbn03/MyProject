-- CLEANING DATA
-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - a few ways

-- Chỉnh lại cột date cho cả 2 bảng cho đồng bộ

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs;

UPDATE layoffs
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM
    world_layoffs.layoffs;

SELECT *
FROM
    world_layoffs.layoffs_2024;
    
UPDATE layoffs_2024
SET `date` = STR_TO_DATE(`date`, '%d/%m/%Y');


-- 1. Remove duplicates
-- - TẠO BẢNG MỚI CÓ CẤU TRÚC TƯƠNG TỰ BẢNG RAW
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- INSERT DATA TỪ BẢNG RAW VÀO BẢNG MỚI
INSERT layoffs_staging
SELECT *
FROM layoffs;

SET SQL_SAFE_UPDATES = 0; -- Tạm thời tắt chế độ safe update khi gặp lỗi Error Code: 1175. 
						-- You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column
                        
SET SQL_SAFE_UPDATES = 1; -- bật lại safe update

UPDATE layoffs_2024
SET layoffs_2024.total_laid_off = NULL
WHERE layoffs_2024.total_laid_off = '';

UPDATE layoffs_2024
SET layoffs_2024.percentage_laid_off = NULL
WHERE layoffs_2024.percentage_laid_off = '';

UPDATE layoffs_2024
SET percentage_laid_off = 1.0
WHERE percentage_laid_off = '1';

INSERT layoffs_staging
SELECT *
FROM layoffs_2024;

-- Thêm cột mới 'ROW_Number' để đếm số hàng trùng nhau
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- STANDARDIZING DATA

SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT *
FROM layoffs_staging2
WHERE industry = ''
	OR industry IS NULL;


SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT DISTINCT(country), TRIM(country)
FROM layoffs_staging2
ORDER BY 1;


SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';



SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num2
FROM layoffs_staging2;

WITH duplicate_cte2 AS
(
SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num2
FROM layoffs_staging2
)
SELECT *
FROM duplicate_cte2
WHERE row_num2 > 1;

CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int,
  `row_num2` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num2
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging3
WHERE row_num2 > 1;

DELETE
FROM layoffs_staging3
WHERE row_num2 > 1;

SELECT *
FROM layoffs_staging3
WHERE company LIKE 'Bally%';

-- now we can convert the data type properly

ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that

-- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging3
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging3;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num2;















