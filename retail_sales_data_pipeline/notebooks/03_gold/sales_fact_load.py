# Databricks notebook source
# DBTITLE 1,sales_fact
spark.sql("""
insert into retailer.gold.sales_fact
select 
  sls.transaction_id,
  cust.customer_id,
  cust.name,
  cust.region,
  prod.product_id,
  prod.category,
  prod.brand,
  sls.quantity,
  sls.price,
  sls.date,
  month(sls.date) as month,
  year(sls.date) as year,
  sls.quantity * sls.price as total_sale_value,
  round(cte.loyalty_score,2) as loyalty_score
from 
  retailer.silver.sales sls
  inner join retailer.silver.customers cust
    on sls.customer_id = cust.customer_id
  inner join retailer.silver.products prod
    on sls.product_id = prod.product_id
  inner join (
    with cte as (
      select 
        customer_id,
        count(*) as frequenacy,
        sum(quantity*price) as monetary,
        max(date) as last_purchase
      from retailer.silver.sales
      where is_current = true
      group by customer_id
    ),
    cte2 as (
      select 
        cte.*,
        date_diff(last_purchase, current_date) as recent_days
      from cte
    ),
    cte3 as (
      select
        cte2.customer_id,
        ((frequenacy*0.4)+(monetary*0.3)+
        case when recent_days < 30 then 10
        when recent_days < 90 then 5 else 0.3 end ) as loyalty_score
      from cte2
    )
    select * from cte3
  ) cte
    on sls.customer_id = cte.customer_id
where 
  sls.is_current = true
  and cust.is_current = true
  and prod.is_current = true
""")