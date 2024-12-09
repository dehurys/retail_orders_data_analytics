
drop table df_orders;

create table df_orders(
[order_id] int primary key
, [order_date] date
, [ship_mode] varchar (20)
, [segment] varchar (20)
, [country] varchar (20)
, [city] varchar (20)
, [state] varchar (20)
, [postal_code] varchar (20)
, [region] varchar (20)
, [category] varchar (20)
, [sub_category] varchar (20)
, [product_id] varchar(50)
, [quantity] int
, [discount] decimal (7,2)
, [sale_price] decimal (7,2)
, [profit] decimal (7,2))

select * from df_orders;

-- ================================

/*** Query 1: find top 10 highest reveue generating products.
***/

SELECT COUNT(product_id) x
, COUNT(distinct(product_id)) y
FROM df_orders;

SELECT TOP 10 
	product_id
	, SUM(sale_price) AS sales
	FROM df_orders
GROUP BY product_id 
ORDER BY 2 DESC;
-- ==================================

/*** Query 2: find top 5 highest selling products in each region.
***/

SELECT DISTINCT (region) from df_orders;
-- -----------
WITH regionalSales AS (
    SELECT region, product_id, SUM(sale_price) AS sales
    FROM df_orders
    GROUP BY region, product_id
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC) AS rn
    FROM regionalSales
) a
WHERE rn <= 5;

-- OR --

WITH regionalSales AS (
    SELECT region, product_id, SUM(sale_price) AS totalSales
    FROM df_orders
    GROUP BY region, product_id
)
, rankedProducts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY totalSales DESC) AS rn
    FROM regionalSales
)
SELECT *
FROM rankedProducts
WHERE rn <= 5;
-- ==============================

/*** Query 3: find month over month growth comparison for 2022 and 2023 sales eg: jan 2022 vs jan 2023
***/

SELECT DISTINCT(YEAR(order_date)) from df_orders;

-- -------------
WITH monthlySales AS (
    SELECT 
        YEAR(order_date) AS orderYear, 
        MONTH(order_date) AS orderMonth, 
        SUM(sale_price) AS totalSales
    FROM df_orders
    GROUP BY YEAR(order_date), MONTH(order_date)
)
SELECT 
    orderMonth, 
    SUM(CASE WHEN orderYear = 2022 THEN totalSales ELSE 0 END) AS sales_2022,
    SUM(CASE WHEN orderYear = 2023 THEN totalSales ELSE 0 END) AS sales_2023
FROM monthlySales
GROUP BY orderMonth
ORDER BY orderMonth;
-- =======================

/*** Query 4: for each category which month had highest sales 
***/

SELECT DISTINCT(category) from df_orders;
-- ---------

WITH categorylSales AS (
	SELECT category
	, FORMAT(order_date, 'yyyyMM') AS order_year_month
	, SUM(sale_price) AS sales
	FROM df_orders
	GROUP BY category, FORMAT(order_date, 'yyyyMM')
-- ORDER BY 2, 3
)
SELECT *
	FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
		FROM categorylSales) a
WHERE rn=1;

--[ OR ]--

WITH categorylSales AS (
	SELECT category
	, FORMAT(order_date, 'yyyyMM') AS order_year_month
	, SUM(sale_price) AS sales
	FROM df_orders
	GROUP BY category, FORMAT(order_date, 'yyyyMM')
-- ORDER BY 2, 3
)
, rankedProducts AS (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
		FROM categorylSales
)
SELECT category, order_year_month, sales
FROM rankedProducts
WHERE rn=1;
-- ===============================

/*** Query 5: which sub category had highest growth by profit in 2023 compare to 2022
***/

WITH yearlySales AS (
    SELECT 
        sub_category
        , YEAR(order_date) AS orderYear
        , SUM(sale_price) AS totalSales
    FROM df_orders
    GROUP BY sub_category, YEAR(order_date)
)
, salesComparison AS (
    SELECT 
        sub_category
        , SUM(CASE WHEN orderYear = 2022 THEN totalSales ELSE 0 END) AS sales2022
        , SUM(CASE WHEN orderYear = 2023 THEN totalSales ELSE 0 END) AS sales2023
    FROM yearlySales
    GROUP BY sub_category
)
SELECT TOP 1 
    sub_category 
    , sales2022
    , sales2023 
    , (sales2023 - sales2022) AS absoluteGrowth
    , ((sales2023 - sales2022) * 100.0 / sales2022) AS growthPercentage
FROM salesComparison
ORDER BY absoluteGrowth DESC, growthPercentage DESC;

-- [which sub category had highest growth-percentage by profit in 2023 compare to 2022] --

WITH yearlySales AS (
    SELECT 
        sub_category
        , YEAR(order_date) AS orderYear
        , SUM(sale_price) AS totalSales
    FROM df_orders
    GROUP BY sub_category, YEAR(order_date)
)
, salesComparison AS (
    SELECT 
        sub_category
        , SUM(CASE WHEN orderYear = 2022 THEN totalSales ELSE 0 END) AS sales2022
        , SUM(CASE WHEN orderYear = 2023 THEN totalSales ELSE 0 END) AS sales2023
    FROM yearlySales
    GROUP BY sub_category
)
SELECT TOP 1 
    sub_category 
    , sales2022
    , sales2023 
    , (sales2023 - sales2022) AS absoluteGrowth
    , ((sales2023 - sales2022) * 100.0 / sales2022) AS growthPercentage
FROM salesComparison
ORDER BY growthPercentage DESC, absoluteGrowth DESC;

/*** So, depending upon the precedence of the outcomes IN ['absoluteGrowth', 'growthPercentage'] in the ORDER BY clause, the output of the query varies for sure.!
Just get the preference which one.
***/

