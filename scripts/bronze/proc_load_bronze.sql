/*
  ====================================================================================
Stored procedure : Load Bronze layer(source -> Bronze)
  ====================================================================================
Scripts Purpose:
  This Stored procedure loads data into the 'bronze' schema from external CSV file
  It performs the following actions : 
      -Truncates the bronze table before loading it
      -Uses the BULK INSERT command to load data from csv files to bronze table
Parameters:
  None 
  This stored procedure doesnot accept any parameter or any return any values


Usage exampleL
  EXEC bronze.load_bronze
*/

CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT'========================================='
		PRINT'Loading Bronze Layer'
		PRINT'========================================='
		PRINT'-----------------------------------------'
		PRINT'Loading CRM Tables'
		PRINT'-----------------------------------------'

		set @batch_start_time=GETDATE();
		set @start_time=GETDATE()
		PRINT'>> Truncating Table : bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info 

		PRINT'>> Inserting into : bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\MUFEEZ\Desktop\Python_DS\DWProjects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		set @end_time=GETDATE()
		PRINT'>> Loading Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds'
		PRINT'-----------------------'

		set @start_time=GETDATE()
		PRINT'>> Truncating Table : bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info 

		PRINT'>> Inserting into : bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\MUFEEZ\Desktop\Python_DS\DWProjects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		set @end_time=GETDATE()
		PRINT'>> Loading Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds'
		PRINT'-----------------------'

		set @start_time=GETDATE()
		PRINT'>> Truncating Table : bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details 

		PRINT'>> Inserting into : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\MUFEEZ\Desktop\Python_DS\DWProjects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		set @end_time=GETDATE()
		PRINT'>> Loading Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds'
		PRINT'-----------------------'

		PRINT'-----------------------------------------'
		PRINT'Loading ERP Tables'
		PRINT'-----------------------------------------'

		set @start_time=GETDATE()
		PRINT'>> Truncating Table : bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12 

		PRINT'>> Inserting into : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\MUFEEZ\Desktop\Python_DS\DWProjects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		set @end_time=GETDATE()
		PRINT'>> Loading Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds'
		PRINT'-----------------------'

		set @start_time=GETDATE()
		PRINT'>> Truncating Table : bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101
	
		PRINT'>> Inserting into : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\MUFEEZ\Desktop\Python_DS\DWProjects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		set @end_time=GETDATE()
		PRINT'>> Loading Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds'
		PRINT'-----------------------'

		set @start_time=GETDATE()
		PRINT'>> Truncating Table : bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 

		PRINT'>> Inserting into : bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\MUFEEZ\Desktop\Python_DS\DWProjects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		set @end_time=GETDATE()
		PRINT'>> Loading Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds'
		PRINT'-----------------------'
		

		set @batch_end_time=GETDATE()
		PRINT'=========================================='
		PRINT'Loading Bronze layer is completed'
		PRINT'	-Total Load Duration : '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+' seconds'
		PRINT'=========================================='
	END TRY
	BEGIN CATCH
		PRINT'============================================'
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT'Error Massage : '+ERROR_MESSAGE()
		PRINT'Error Massage : '+CAST(ERROR_NUMBER()AS NVARCHAR)
		PRINT'Error Massage : '+CAST(ERROR_STATE()AS NVARCHAR)

		PRINT'============================================'
	END CATCH
END

