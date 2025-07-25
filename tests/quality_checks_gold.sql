-- check the quality of the gold.dim_customer

--check are they have any duplication cst_id

SELECT cst_id,COUNT(*) 
FROM(
	SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.gen,
	ca.bdate,
	la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key=ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key=la.cid
)t GROUP BY cst_id
having count(*)>1



--Check the data intergrations
SELECT 
DISTINCT 
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr
	 ELSE COALESCE(ca.gen,'n/a')
END as new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
ORDER BY 1,2




--check the any duplication values
SELECT prd_id ,COUNT(*)
FROM(
SELECT 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
)t GROUP BY prd_id
having COUNT(*)>1



SELECT * FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON fs.customer_key= dc.customer_key
LEFT JOIN gold.dim_products dp
ON fs.product_key =dp.product_key
WHERE dp.product_key IS NULL

   
