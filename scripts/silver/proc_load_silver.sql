/*
  ===============================================================
 Stored procedure: Load silver Layer(Bronze->Silver)
  ===============================================================
Scripts purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) Process to populate 
    the 'silver' schema tables from the 'bronze' schema
  Action Performed
    -Truncate silver table
    -insert Transform and cleaned data from bronze into silver tables
  
Parameters :
  None 
  This stored procedure doesnot accept any paramters or return any values 

usage example:
  EXEC silver.load_silver;
  ====================================================================
*/

EXEC silver.load_silver


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @StartTime DATETIME, @EndTime DATETIME ,@BatchStartTime DATETIME,@BatchEndTime DATETIME

	BEGIN TRY
		SET @BatchStartTime=GETDATE()
		PRINT'============================================';
		PRINT'Loading Silver Layer'
		PRINT'============================================';

		PRINT'--------------------------------------------';
		PRINT'Loading CRM Tables'
		PRINT'--------------------------------------------';

		--Loading silver.crm_cust_info table
		SET @StartTime=GETDATE()
		PRINT'>> Truncating Table : silver.crm_cust_info'
		Truncate Table silver.crm_cust_info
		PRINT'>> Inserting Data into : silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(cst_id,
									cst_key,
									cst_firstname,
									cst_lastname,
									cst_marital_status,
									cst_gndr,
									cst_create_date)

			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname)as cst_firstname,
			TRIM(cst_lastname)as cst_lastname,
			CASE WHEN TRIM(UPPER(cst_marital_status))='M' THEN 'Married'
				 WHEN TRIM(UPPER(cst_marital_status))='S' THEN 'Single'
				 ELSE 'n/a'
			END as cst_marital_status,
			CASE WHEN TRIM(UPPER(cst_gndr))='M' THEN 'Male'
				 WHEN TRIM(UPPER(cst_gndr))='F' THEN 'Female'
				 ELSE 'n/a'
			END as cst_gndr ,
			cst_create_date

			FROM (

			SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)as flag_last
			from silver.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t WHERE flag_last =1
			SET @EndTime =GETDATE()
			PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR)+' Seconds'
			PRINT'>> -------------'

			--Loading silver.crm_prd_info table
			SET @StartTime=GETDATE()
			PRINT'>> Truncating Table : silver.crm_prd_info'
			Truncate Table silver.crm_prd_info
			PRINT'>> Inserting Data into : silver.crm_prd_info'
			INSERT INTO silver.crm_prd_info(prd_id,
											cat_id,
											prd_key,
											prd_nm,
											prd_cost,
											prd_line,
											prd_start_dt,
											prd_end_dt)

			SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key))as prd_key,
			prd_nm,
			CASE WHEN prd_cost<0 or prd_cost is NULL then 0
				 ELSE prd_cost
			END as prd_cost,
			CASE TRIM(UPPER(prd_line))
				 WHEN 'R' THEN 'Road'
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'n/a'
			END as prd_line,
			CAST(CAST(prd_start_dt AS VARCHAR) as DATE)as prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 as DATE) as prd_end_dt

			 FROM silver.crm_prd_info
			SET @EndTime =GETDATE()
			PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR)+' Seconds'
			PRINT'>> -------------'

			--Loading silver.crm_sales_details table
			SET @StartTime=GETDATE()
			PRINT'>> Truncating Table : silver.crm_sales_details'
			Truncate Table silver.crm_sales_details
			PRINT'>> Inserting Data into : silver.crm_sales_details'
			INSERT INTO silver.crm_sales_details(sls_ord_num,
												 sls_prd_key,
												 sls_cust_id,
												 sls_order_dt,
												 sls_ship_dt,
												 sls_due_dt,
												 sls_sales,
												 sls_quantity,
												 sls_price)
		
					SELECT 
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					CASE WHEN sls_order_dt<=0 OR LEN(sls_order_dt)!=8 THEN NULL
						 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
					END as sls_order_dt,
					CASE WHEN sls_ship_dt<=0 OR LEN(sls_ship_dt)!=8 THEN NULL
						 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
					END as sls_ship_dt,
					CASE WHEN sls_due_dt<=0 OR LEN(sls_due_dt)!=8 THEN NULL
						 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
					END as sls_due_dt,
					CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=ABS(sls_quantity)*ABS(sls_price) then ABS(sls_price)*ABS(sls_quantity)
						 ELSE sls_sales
					END as sls_sales,
					sls_quantity,
					CASE WHEN sls_price is NULL or sls_price<=0 then sls_sales/NULLIF(sls_quantity,0)
						ELSE sls_price
					END as sls_price

					FROM silver.crm_sales_details
			SET @EndTime =GETDATE()
			PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR)+' Seconds'
			PRINT'>> -------------'


			PRINT'--------------------------------------------';
			PRINT'Loading ERP Tables'
			PRINT'--------------------------------------------';

			--Loading silver.erp_cust_az12 table
			SET @StartTime=GETDATE()
			 PRINT'>> Truncating Table : silver.erp_cust_az12'
			Truncate Table silver.erp_cust_az12
			PRINT'>> Inserting Data into : silver.erp_cust_az12'
			INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

				SELECT 
				CASE WHEN cid LIKE 'NAS%' then SUBSTRING(cid,4,len(cid)) --remove leading NAS 
					ELSE cid
				END as cid,
				CASE WHEN bdate >GETDATE() THEN NULL   --bdate should be smaller than current date
					ELSE bdate
				END as bdate,
				CASE WHEN TRIM(UPPER(gen)) in('F', 'FEMALE') THEN 'Female' 
					WHEN TRIM(UPPER(gen)) in ('M','MALE') THEN 'Male'
					ELSE 'n/a'
				END as gen  --standarization and consistency
				FROM silver.erp_cust_az12
			SET @EndTime =GETDATE()
			PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR)+' Seconds'
			PRINT'>> -------------'

			--Loading silver.erp_loc_a101 table
			SET @StartTime=GETDATE()
			PRINT'>> Truncating Table : silver.erp_loc_a101'
			Truncate Table silver.erp_loc_a101
			PRINT'>> Inserting Data into : silver.erp_loc_a101'
			INSERT INTO silver.erp_loc_a101(cid,cntry)
				SELECT 
				REPLACE(cid,'-','')as cid,

				CASE WHEN TRIM(UPPER(cntry)) =  'AUSTRALIA' THEN 'Australia'
					 WHEN TRIM(UPPER(cntry)) IN ('USA','UNITED STATES','US') THEN 'United States'
					 WHEN TRIM(UPPER(cntry)) IN('DE','GERMANY') THEN 'Germany'
					 WHEN TRIM(UPPER(cntry)) =  'UNITED KINGDOM' THEN 'United Kingdom'
					 WHEN TRIM(UPPER(cntry)) = 'CANADA' THEN 'Canada'
					 WHEN TRIM(UPPER(cntry)) = 'FRANCE' THEN 'France'
					 ELSE 'n/a'
				END as cntry

				FROM silver.erp_loc_a101



			SET @EndTime =GETDATE()
			PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR)+' Seconds'
			PRINT'>> -------------'

			--Loading silver.erp_px_cat_g1v2 table
			SET @StartTime=GETDATE()
			PRINT'>> Truncating Table : silver.erp_px_cat_g1v2'
			Truncate Table silver.erp_px_cat_g1v2
			PRINT'>> Inserting Data into : silver.erp_px_cat_g1v2'
			INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
				SELECT 
					id,
					cat,
					subcat,
					maintenance
				FROM silver.erp_px_cat_g1v2
			SET @EndTime =GETDATE()
			PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR)+' Seconds'
			PRINT'>> -------------'

			SET @BatchEndTime=GETDATE();
			PRINT'====================================================='
			PRINT'Loading Silver Layer is Completed'
			PRINT'Total Load Duration : '+CAST(DATEDIFF(SECOND,@BatchStartTime,@BatchEndTime)AS VARCHAR)+' Seconds'
			PRINT'====================================================='
		END TRY
		BEGIN CATCH
			PRINT'====================================================='
			PRINT'ERROR OCCURED DURING LOADING SILVER LAYER'
			PRINT'Error Message '+ERROR_MESSAGE()
			PRINT'Error Message '+CAST(ERROR_NUMBER()AS NVarchar)
			PRINT'Error Message '+CAST(ERROR_STATE()AS NVARCHAR)
			PRINT'====================================================='
		END CATCH
END
