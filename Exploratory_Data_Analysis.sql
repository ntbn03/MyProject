-- Exploratory Data

SELECT *
FROM layoffs_staging4;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging4;

SELECT *
FROM layoffs_staging4
WHERE percentage_laid_off = '1'
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging4;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging4
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY country
ORDER BY 2 DESC;

SELECT location, SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY location
ORDER BY 2 DESC;


SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging4
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging4
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total
FROM layoffs_staging4
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, total
, SUM(total) OVER (ORDER BY `month`) AS rolling_total
FROM Rolling_Total;

-- SUM(total) OVER (ORDER BY `month`) AS rolling_total 
-- 		sử dụng hàm cửa sổ (window function) SUM để tính tổng luỹ kế.
-- 		tính tổng của 'total' theo luỹ kế tháng 


SELECT company, SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


WITH Company_Year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging4
GROUP BY company, YEAR(`date`)
),
Company_Years_Rank AS
(
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Years_Rank
WHERE Ranking <= 5;

-- TỔNG SỐ NGƯỜI SA THẢI VÀ TỈ LỆ SA THẢI THEO NGÀNH TRONG GIAI ĐOẠN 2020 - 08/2024
SELECT industry, SUM(total_laid_off), ROUND(AVG(percentage_laid_off), 2)
FROM layoffs_staging4
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY 2 DESC;














