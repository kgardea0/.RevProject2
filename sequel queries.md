1a) What is the top selling category of items?

SELECT product_category AS category, product(qty * price) AS sales
FROM complete_csv
GROUP BY product_category
ORDER BY sales DESC

1b) Per Country?


2a)



3) Which locations see the highest traffic of sales?


SELECT country, city, SUM(qty) AS 'quantity'
FROM complete_csv
GROUP BY country, city

plotBarChart(df, true, "product_category", "Top Selling Products by Country")