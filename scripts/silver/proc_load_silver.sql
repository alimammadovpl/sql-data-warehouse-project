-- Stored procedure to transform and load cleaned data from Bronze to Silver layer, handling standardization, deduplication, and simple business logic.


CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN 
    BEGIN TRY
		TRUNCATE TABLE Silver.crm_cust_info;
		INSERT INTO Silver.crm_cust_info(cst_id,
			   cst_key,
			   cst_firstname,
			   cst_lastname,
			   cst_material_status,
			   cst_gndr,
			   cst_create_date)

		SELECT cst_id,
			   cst_key,
			   TRIM(cst_firstname),
			   TRIM(cst_lastname),
			   CASE WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married'
					WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single'
			   ELSE 'n/a' END AS cst_material_status,
	   
			   CASE WHEN TRIM(Upper(cst_gndr))='M' THEN 'Male'
					WHEN TRIM(UPPER(cst_gndr))='F' THEN 'Female'
			   ELSE 'n/a' END AS cst_gndr,
			   cst_create_date

		FROM (select *,row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from Bronze.crm_cust_info
		where cst_id is not null)t
		where flag_last=1

		TRUNCATE TABLE Silver.crm_prd_info;
		INSERT INTO Silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)

		select 
			 prd_id,
		replace(substring(prd_key,1,5),'-','_') cat_id,
		substring(prd_key,7,len(prd_key)) prd_key,
			 prd_nm,
			 ISNULL(prd_cost,0) prd_cost,
		CASE UPPER(TRIM(prd_line))
			  WHEN 'M' THEN 'Mountain'
			  WHEN 'R' THEN 'Road'
			  WHEN 'S' THEN 'Other Sales'
			  WHEN 'T' THEN 'Touring'
			 ELSE 'n/a' END AS prd_line,
			 CAST(prd_start_dt AS DATE)  prd_start_dt,
			 CAST(LEAD(prd_start_dt) over(PARTITION BY prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt

		from bronze.crm_prd_info

		TRUNCATE TABLE Bronze.crm_sales_details;
		insert into Silver.crm_sales_details(
		sls_ord_num,
			  sls_prd_key,
			  sls_cust_id,
			  sls_order_dt,
			  sls_ship_dt,
			  sls_due_dt,
			  sls_sales,
			  sls_quantity,
			  sls_price)
		select 
			  sls_ord_num,
			  sls_prd_key,
			  sls_cust_id,
			  CAST(sls_order_dt AS DATE) sls_order_dt,
			  CAST(sls_ship_dt AS DATE) sls_ship_dt,
			  CAST(sls_due_dt AS DATE) sls_due_dt,
		  CASE WHEN sls_sales!=sls_quantity*ABS(sls_price) or sls_sales<=0 or sls_sales is null THEN sls_quantity*ABS(sls_price)
		  ELSE sls_sales END sls_sales,
		  sls_quantity,
		  CASE WHEN sls_price is null or sls_price<=0 then     
			  sls_sales/nullif(sls_quantity,0)
			  else sls_price end sls_price

		from Bronze.crm_sales_details

		TRUNCATE TABLE Bronze.erp_CUST_AZ12;
		INSERT INTO Silver.erp_CUST_AZ12(
		cid,
		bdate,
		gen)


		select 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,len(cid)) 
		else cid end as cid,
		case when bdate>getdate() then null
		else bdate end as bdate,
		case 
		when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
		WHEN upper(trim(gen)) in ('M','MALE') THEN 'Male'
		else  'n/a'
		 end as
		gen

		from Bronze.erp_CUST_AZ12

		TRUNCATE TABLE bronze.erp_loc_a101;
		INSERT INTO Silver.erp_loc_a101(
		cid,
		cntry)

		select 
		replace(cid,'-','') cid,
			case when trim(cntry)='DE' THEN 'Germany'
				 when trim(cntry) in('US','USA') THEN 'United States'
				 when trim(cntry)='' or trim(cntry) is null then 'n/a'
				 else trim(cntry) end as
		 cntry



		 from bronze.erp_loc_a101

		 TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		 insert into silver.erp_px_cat_g1v2(
			  id,
			  cat,
			  subcat,
			  maintenance)
		select 
			  id,
			  cat,
			  subcat,
			  maintenance

		from bronze.erp_px_cat_g1v2

END TRY
	BEGIN CATCH
	PRINT '====================================='
	PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER!'
	PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
	PRINT 'ERROR MESSAEGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '======================================'
	END CATCH
END


 













