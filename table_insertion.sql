
-- LOAD CATEGORY INFORMATION
INSERT INTO
    CATEGORY_DIM (name)
SELECT
    DISTINCT category
from
    nike_raw;
    
-- LOAD TYPE INFORMATION
INSERT INTO
    PRODUCT_TYPE_DIM (name)
SELECT
    DISTINCT type
from
    nike_raw;

-- LOAD PRODUCT INFORMATION
INSERT INTO
    PRODUCT_DIM
SELECT
    DISTINCT productid,
    title,
    subtitle,
    cd.id as category,
    pt.id as type,
    currentprice,
    fullprice
FROM
    nike_raw nr
    JOIN 
        CATEGORY_DIM cd ON cd.name = nr.category
    JOIN 
        PRODUCT_TYPE_DIM pt ON pt.name = nr.type;

select * from product_dim;

-- LOAD COLOR LABEL INFORMATION
INSERT INTO
    COLOR_LABEL_DIM (name)
SELECT
    DISTINCT "color-Label"
from
    nike_raw;

    
-- LOAD COLOR INFORMATION
INSERT INTO
    COLOR_DIM
SELECT
    DISTINCT 
    "color-ID",
    "color-Discount",
    cl.id
FROM
    nike_raw nr
    JOIN COLOR_LABEL_DIM cl ON cl.name = nr."color-Label";

-- LOAD SALE FACT INFORMATION

INSERT INTO SALE_FACT
-- GET THE AMOUNT OF SALES FIRST
WITH cte_total_sales AS (
    SELECT
        pd.id as product,
        COUNT(pd.id) as total_sales
    FROM
        nike_raw nr
    JOIN PRODUCT_DIM pd ON pd.id = nr.productid
    group by pd.id
)
-- GET THE TOTAL_SALES BY UID, PRODUCT AND COLOR from raw and CTE
SELECT
    nr.uid,
    COUNT(pd.id) AS amount,
    pd.id as product,
    cd.id as category_id,
    cold.id as color_id,
    SUM(pd.current_price * ts.total_sales) as total_sales
FROM
    nike_raw nr
    JOIN PRODUCT_DIM pd ON pd.id = nr.productid
    JOIN cte_total_sales ts ON ts.product = nr.productid
    JOIN category_dim cd ON cd.name = nr.category
    JOIN color_dim cold ON cold.id = nr."color-ID"
GROUP BY
    nr.uid,pd.id,cd.id,cold.id;
