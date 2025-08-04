CREATE OR ALTER PROCEDURE silver.load_silver_erp
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();
    
    BEGIN TRY
        PRINT '======================================';
        PRINT 'Loading ERP data into Silver layer';
        PRINT '======================================';
        ----- FULL LOAD SQL SCRIPT -----
        SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_cust_az12'
    TRUNCATE table silver.erp_cust_az12;

    PRINT '>> Inserting Data into: silver.erp_cust_az12'
    INSERT into silver.erp_cust_az12
    (
    cid,
    bdate,
    gen
    )
    SELECT
    case when cid like 'NAS%' then substring(cid,4,len(cid))
        else cid
    end cid,
    case when bdate > GETDATE() then NULL
        else bdate
    end bdate,
    case when UPPER(trim(gen)) in ('F','Female') then 'Female'
        when UPPER(trim(gen)) in ('M','Male') then 'Male'
        else 'n/a'
    end gen
    from bronze.erp_cust_az12;

    set @end_time = GETDATE();
    PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
    PRINT '------------------------------------------';
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_loc_a101'
    TRUNCATE table silver.erp_loc_a101;

    PRINT '>> Inserting Data into: silver.erp_loc_a101'
    INSERT into silver.erp_loc_a101
    (
    cid,
    cntry
    )
    SELECT
        REPLACE(cid,'-','') as cid,
        CASE when TRIM(cntry) = 'DE' then 'Germany'
            when TRIM(cntry) in ('US','USA') then 'United States'
            when TRIM(cntry) = '' or cntry is NULL then 'n/a'
            else TRIM(cntry)
        end as cntry
    FROM bronze.erp_loc_a101;
    set @end_time = GETDATE();
    PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
    PRINT '------------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
    TRUNCATE table silver.erp_px_cat_g1v2;

    PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2'
    INSERT into silver.erp_px_cat_g1v2
    (
    id,
    cat,
    subcat,
    maintenance
    )
    SELECT
    id,
    cat,
    subcat,
    maintenance
    from bronze.erp_px_cat_g1v2;
    set @end_time = GETDATE();
    PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
    PRINT '------------------------------------------';
    SET @batch_end_time = GETDATE();
    PRINT '=======================================';
    PRINT 'ERP Load Summary';
    PRINT '=======================================';
    PRINT '>> Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';
    END TRY
    BEGIN CATCH
      PRINT '=======================================';
      PRINT 'Error occurred while loading ERP data';
      PRINT '=======================================';
      PRINT 'Error Message: ' + ERROR_MESSAGE();
      PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
      PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
    END CATCH
END