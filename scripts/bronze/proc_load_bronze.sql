CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
    BEGIN TRY
	    SET @batch_start_time=GETDATE()
		PRINT '=========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=========================================';
    
		PRINT '-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------';
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ME\OneDrive\Masaüstü\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
        PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ME\OneDrive\Masaüstü\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ME\OneDrive\Masaüstü\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			 ROWTERMINATOR = '\n',
			TABLOCK,
			KEEPNULLS
		);
	    SET @end_time=GETDATE();
		PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		PRINT '-----------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------';
	    
		SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ME\OneDrive\Masaüstü\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
	    SET @end_time=GETDATE();
		PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ME\OneDrive\Masaüstü\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
	    SET @end_time=GETDATE();
		PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';

		SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ME\OneDrive\Masaüstü\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	    SET @end_time=GETDATE();
		PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		
		SET @batch_end_time=GETDATE()
		PRINT 'Total Load Duration:'+CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
	END TRY
	BEGIN CATCH
	PRINT '=========================================';
	PRINT 'Error Occured During Loading Bronze Layer';
	PRINT '========================================='
	END CATCH
END
