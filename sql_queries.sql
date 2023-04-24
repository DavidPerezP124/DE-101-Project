

--1 Query the top 5 sales by product
--1.1 TOP 5 MOST SOLD PRODUCT
SELECT
    pd.title,
    SUM(amount) as sales
FROM sale_fact sf
JOIN 
    product_dim pd ON pd.id = sf.product_id
GROUP BY  pd.title
ORDER BY sales DESC
LIMIT 5;

--1.2 EXTRA TOP 5 MOST REVENUE PER PRODUCT
SELECT
    pd.title,
    SUM(total_sales) as sales
FROM sale_fact sf
JOIN 
    product_dim pd ON pd.id = sf.product_id
GROUP BY  pd.title
ORDER BY sales DESC
LIMIT 5;

-- Query the top 5 sales by category agrupation
SELECT
    cd.name,
    SUM(amount * total_sales) as sales
FROM sale_fact sf
JOIN 
    category_dim cd ON cd.id = sf.category_id
GROUP BY cd.name
ORDER BY sales DESC
LIMIT 5;


-- Query the least 5 sales by category agrupation
SELECT
    cd.name,
    SUM(amount * total_sales) as sales
FROM sale_fact sf
JOIN 
    category_dim cd ON cd.id = sf.category_id
GROUP BY cd.name
ORDER BY sales
LIMIT 5;

-- Query the top 5 sales by title and subtitle agrupation
SELECT
    pd.title,
    pd.subtitle,
    SUM(amount * total_sales) as sales
FROM sale_fact sf
JOIN 
    product_dim pd ON pd.id = sf.product_id
GROUP BY  pd.title, pd.subtitle
ORDER BY sales DESC
LIMIT 5;

-- Query the top 3 products that has greatest sales by category
WITH ranked_cte AS (
    SELECT
        pd.title as title,
        cd.name as category,
        SUM(amount  * total_sales) as sales, 
        RANK() OVER (PARTITION BY sf.category_id ORDER BY sales DESC) as ranking
    FROM sale_fact sf
    JOIN 
        product_dim pd ON pd.id = sf.product_id
    JOIN 
        category_dim cd ON cd.id = sf.category_id
    GROUP BY 
        title,category,sf.category_id
)
SELECT
    title, category, sales
FROM ranked_cte
WHERE ranking = 1
ORDER BY sales DESC
LIMIT 3;

-- EXTRA: BEST SELLING COLOR PER PRODUCT
WITH ranked_cte AS (
    SELECT
        pd.title as title,
        cd.id as color,
        SUM(amount  * total_sales) as sales, 
        RANK() OVER (PARTITION BY sf.color_id ORDER BY sales DESC) as ranking
    FROM sale_fact sf
    JOIN 
        product_dim pd ON pd.id = sf.product_id
    JOIN 
        color_dim cd ON cd.id = sf.color_id
    GROUP BY 
        title,category,sf.category_id
)
SELECT
    title, category, sales
FROM ranked_cte
WHERE ranking = 1
ORDER BY sales DESC
LIMIT 3;