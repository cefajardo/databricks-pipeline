-- A. Create the bronze streaming table

CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze
    COMMENT "Ingest order CSV files from cloud storage"
    TBLPROPERTIES (
        "quality" = "bronze"
        --,"pipelines.reset.allowed" = false
    )
AS
SELECT
    *,
    current_timestamp() AS processing_time,
    _metadata.file_name AS source_file
FROM STREAM read_files(
    "${source}/orders",
    header => true,
    format => 'CSV'
);

-- B. Create the silver streaming table
-- Be carefull with FROM STREAM keyword

CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver
    (
        CONSTRAINT valid_notification EXPECT (notifications IN ('Y', 'x')),
        CONSTRAINT valid_date EXPECT (order_timestamp > '2021-12-25') ON VIOLATION DROP ROW,
        CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
    )
    COMMENT "Silver clean order data"
    TBLPROPERTIES ( "quality" = "silver" )
AS
SELECT
    order_id,
    timestamp(order_timestamp) AS order_timestamp,
    customer_id,
    notifications
FROM STREAM 1_bronze_db.orders_bronze;

-- C. Create the materialized view aggregation from the orders_silver table

CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date
    COMMENT "Aggregate order gold data for downstream analysis"
    TBLPROPERTIES ( "quality" = "gold" )
AS
SELECT
    date(order_timestamp) AS order_date,
    count(*) AS total_daily_orders
FROM 2_silver_db.orders_silver
GROUP BY date(order_timestamp);

