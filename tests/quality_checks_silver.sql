/*
=======================================================
Quality checks
=======================================================
Scripts purpose
	this scripts performs various quality checks for data consistency,accuracy,
	and standarization acrross the silver schema. It includes checks for:
		-Null or duplicats values
		-unwanted spaces in string field
		-data standarization and consistency
		-invalid date range and orders
		-Date consistency between related fields
Usage Notes:
	-Run these checks after data loading silver layer
	-investigate and resolve any discrepancies found during the checks
	

*/



--===============================================
--checking silver.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2 WHERE id NOT IN
(SELECT cat_id FROM silver.crm_prd_info)

--removing unwanted spaces
SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2
WHERE cat!=TRIM(cat)
SELECT DISTINCT subcat FROM silver.erp_px_cat_g1v2
WHERE subcat!=TRIM(subcat)


--===============================================
  --checking silver.crm_prd_info
--===============================================
--check the null or duplicates key 
--Expectation : No result
SELECT prd_id,COUNT(*)
from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null


--check unwanted spaces 
SELECT prd_nm from silver.crm_prd_info
WHERE  prd_nm!=TRIM(prd_nm)

--check the product cost
SELECT prd_cost from silver.crm_prd_info
WHERE prd_cost<0 or prd_cost is NULL




--=========================================
-- checking silver.crm_cust_info
--=========================================
--check for unwanted spaces

SELECT cst_key FROM silver.crm_cust_info
WHERE cst_key!=TRIM(cst_key)

SELECT cst_firstname FROM silver.crm_cust_info
WHERE cst_firstname!=TRIM(cst_firstname)

SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname!=TRIM(cst_lastname)

--standarization and consistance

SELECT DISTINCT 
	cst_marital_status as old,
	CASE WHEN TRIM(UPPER(cst_marital_status))='M' THEN 'Married'
	 WHEN TRIM(UPPER(cst_marital_status))='S' THEN 'Single'
	 ELSE 'n/a'
END as cst_marital_status from silver.crm_cust_info

SELECT DISTINCT 
	cst_gndr as old,
	

