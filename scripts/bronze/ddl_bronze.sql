 IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
 DROP TABLE bronze.crm_cust_info;
 CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL
 DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT PRIMARY KEY,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   VARCHAR(20),
    sls_prd_key   VARCHAR(20),
    sls_cust_id   INT,  
    sls_order_dt  DATETIME,  
    sls_ship_dt   DATETIME,  
    sls_due_dt    DATETIME,  
    sls_sales     INT, 
    sls_quantity  INT,  
    sls_price     INT  
);

IF OBJECT_ID ('bronze.erp_cust_az12' , 'U') IS NOT NULL
 DROP TABLE Bronze.erp_CUST_AZ12;
CREATE TABLE Bronze.erp_CUST_AZ12 (
    cid VARCHAR(20) PRIMARY KEY,
    bdate DATE NOT NULL,
    gen VARCHAR(10) NOT NULL
);

IF OBJECT_ID ('bronze.erp_loc_a101' , 'U') IS NOT NULL
 DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid VARCHAR(20) PRIMARY KEY,
    cntry VARCHAR(50) NOT NULL
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL
 DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(20) PRIMARY KEY,
    cat VARCHAR(50) NOT NULL,
    subcat VARCHAR(50) NOT NULL,
    maintenance VARCHAR(3) NOT NULL
);
