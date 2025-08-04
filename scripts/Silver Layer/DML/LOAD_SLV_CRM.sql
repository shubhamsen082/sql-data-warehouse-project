CREATE OR ALTER PROCEDURE silver.load_silver_crm
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();
    
    BEGIN TRY
        PRINT '======================================';
        PRINT 'Loading CRM data into Silver layer';
        PRINT '======================================';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info'
     TRUNCATE table silver.crm_cust_info;

     PRINT '>> Inserting Data into: silver.crm_cust_info'
     INSERT into silver.crm_cust_info
     (cst_id,
     cst_key,
     cst_first_name, 
     cst_last_name,
     cst_material_status, 
     cst_gndr, 
     cst_created_date)
     select 
     cst_id,
     cst_key,
     TRIM(cst_first_name) as cst_first_name,
     TRIM(cst_last_name) as cst_last_name,
     CASE when UPPER(TRIM(cst_material_status)) = 'M' then 'MARRIED'
          when UPPER(TRIM(cst_material_status)) = 'S' then 'SINGLE'
          else 'n/a'
     END cst_material_status, ---- Normalized Marital Status values into readable format
     CASE when UPPER(TRIM(cst_gndr)) = 'M' then 'MALE'
          when UPPER(TRIM(cst_gndr)) = 'F' then 'FEMALE'
          else 'n/a'
     END cst_gndr,  --- Normalize gender values into readable format
     cst_created_date
     from
     (SELECT *,
     ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_created_date DESC) as rn
     FROM bronze.crm_cust_info where cst_id is not null
     )t where t.rn =1;  ---- select the record as per customer latest date

    set @end_time = GETDATE();
    PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
    PRINT '------------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_prd_info'
    TRUNCATE TABLE silver.crm_prd_info;
    
    PRINT '>> Inserting Data into: silver.crm_prd_info'
    INSERT into silver.crm_prd_info(
     prd_id,
     cat_id,
     prd_key,
     prd_nm,  
     prd_cost,
     prd_line,
     prd_start_dt,
     prd_end_dt
     )
     SELECT 
     prd_id,
     REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
     SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,  -- Changed alias
     prd_nm,
     ISNULL(prd_cost,0) as prd_cost,
     case UPPER(TRIM(prd_line))
          when 'M' then 'MOUNTAIN'
          when 'R' then 'ROAD'
          when 'T' then 'TOURING'
          when 'S' then 'OTHER'
          else 'n/a'
     END as prd_line,
     prd_start_dt,
     DATEADD(day, -1, LEAD(prd_start_dt) OVER 
          (PARTITION BY SUBSTRING(prd_key,7,LEN(prd_key)) ORDER BY prd_start_dt)
     ) as prd_end_dt
     from bronze.crm_prd_info;

    set @end_time = GETDATE();
    PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
    PRINT '------------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_sales_details'
    TRUNCATE TABLE silver.crm_sales_details;
    
    PRINT '>> Inserting Data into: silver.crm_sales_details'
    INSERT into silver.crm_sales_details(
     sls_ord_num,
     sls_prd_key,
     sls_cust_id,
     sls_order_dt,
     sls_ship_dt,
     sls_due_dt,
     sls_sales,
     sls_quantity,
     sls_price
     )
     SELECT 
     sls_ord_num,
     sls_prd_key,
     sls_cust_id,
     case when sls_order_dt = 0 or LEN(sls_order_dt)!=8 then NULL
          else CAST(CAST(sls_order_dt as varchar) as DATE)
     END as sls_ship_dt,
     case when sls_ship_dt = 0 or LEN(sls_ship_dt)!=8 then NULL
          else CAST(CAST(sls_ship_dt as varchar) as DATE)
     END as sls_ship_dt,
     case when sls_due_dt = 0 or LEN(sls_due_dt)!=8 then NULL
               else CAST(CAST(sls_due_dt as varchar) as DATE)
          END as sls_due_dt,
     CASE when sls_sales IS NULL or sls_sales <=0 or (sls_sales != sls_quantity * abs(sls_price))
          THEN sls_quantity * abs(sls_price)
          else sls_sales
          END as sls_sales,
          sls_quantity,
     CASE when sls_price is NULL or sls_price <=0
          THEN sls_sales/NULLIF(sls_quantity,0)
          else sls_price
          END as sls_price
     FROM bronze.crm_sales_details;

    set @end_time = GETDATE();
    PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
    PRINT '------------------------------------------';
    
    SET @batch_end_time = GETDATE();
    PRINT '=======================================';
    PRINT 'CRM Load Summary';
    PRINT '=======================================';
    PRINT '>> Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';
    END TRY
    BEGIN CATCH
        PRINT '=======================================';
        PRINT 'Error occurred while loading CRM data';
        PRINT '=======================================';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
    END CATCH
END
