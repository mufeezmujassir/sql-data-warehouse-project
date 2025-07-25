/*
============================================
DDL Scripts: Create Gold Views
============================================
Scripts purpose:
  This layer create view for the Gold layer in the data warehouse
  The Gold layer represents the final dimension and fact tables (Star schema)

  Each view performs transformations and combines data from the silver layer 
  to produce a clean, enriched and business-ready dataset

  usage : 
      - These views can be quried directly for analytics and reporting
=============================================
*/


--====================================
--create  Dimension : gold.dim_customers
--=====================================
if OBJECT_ID('gold.dim_customers','V')IS NOT NULL
	drop VIEW gold.dim_customers

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER()OVER(ORDER BY cst_id)AS Customer_key, --create the surround key for table creation
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
	END as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid


  
--====================================
--create  Dimension : gold.dim_products
--=====================================

if OBJECT_ID('gold.dim_products','V')IS NOT NULL
	drop VIEW gold.dim_products

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER()OVER(ORDER BY prd_key,prd_start_dt)AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS sub_category,
	pc.maintenance ,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt As start_date
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL --filter out all historical data


--====================================
--create  Dimension : gold.fact_sales
--=====================================

if OBJECT_ID('gold.fact_sales','V')IS NOT NULL
	drop VIEW gold.fact_sales


CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales As sales_amount,
	sd.sls_quantity As quantity,
	sd.sls_price AS price

from silver.crm_sales_details sd
LEFT  JOIN gold.dim_products dp
ON sd.sls_prd_key=dp.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id=dc.customer_id
