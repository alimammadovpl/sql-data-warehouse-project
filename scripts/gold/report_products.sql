CREATE VIEW gold.report_products AS
WITH CTE AS(
select 
p.product_key as product_key,
p.product_name as product_name,
p.category as category,
p.subcategory as subcategory,
p.cost as cost,
max(order_date) last_sale_date,
count(distinct order_number) total_orders,
SUM(sales_amount) as total_sales,
sum(sales_quantity) total_quantity_sold,
count(distinct customer_key) total_customers,
DATEDIFF(month,min(order_date),max(order_date)) lifespan,
DATEDIFF(month,max(order_date),getdate()) recency_inmonths,
ROUND(AVG(CAST(sales_amount AS FLOAT)/NULLIF(sales_quantity,0)),1) avg_selling_price
from gold.fact_sales f
left join gold.dim_products p
ON f.product_key=p.product_key
where order_date is not null
group by 
p.product_key,
p.product_name,
p.category,
p.subcategory,
p.cost)

SELECT 
product_key,
product_name,
category,
subcategory,
cost,
last_sale_date,
recency_inmonths,
CASE
    WHEN total_sales>50000 THEN 'High Performer'
	WHEN total_sales>=10000 THEN 'Mid-Range'
	ELSE 'Low Performer' END AS product_segment,
lifespan,
total_orders,
total_sales,
total_quantity_sold,
total_customers,
avg_selling_price,
CASE
   WHEN total_orders=0 THEN 0
   ELSE total_sales/total_orders END AS avg_order_revenue,
CASE
   WHEN lifespan=0 then total_sales
   ELSE total_sales/lifespan
   END AS avg_monthly_revenue




FROM CTE

