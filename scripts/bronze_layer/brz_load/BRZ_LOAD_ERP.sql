CREATE OR ALTER PROCEDURE bronze.load_bronze_erp AS
BEGIN
   DECLARE @start_time DATETIME, @end_time DATETIME;
   DECLARE @overall_start DATETIME, @overall_end DATETIME;
   BEGIN TRY
      PRINT '======================================';
      PRINT 'Loading ERP data into Bronze layer';
      PRINT '======================================';

      set @overall_start = GETDATE();

      -- Load BRONZE.erp_cust_az12
      set @start_time = GETDATE();
      PRINT ' >> Truncating table BRONZE.erp_cust_az12 ';
      TRUNCATE TABLE BRONZE.erp_cust_az12;

      PRINT ' >> Inserting Data : BRONZE.erp_cust_az12 ';
      BULK INSERT BRONZE.erp_cust_az12
      FROM 'E:\Data_SQL\Project_files\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
      WITH (
          FIELDTERMINATOR = ',',
          FIRSTROW = 2,
          tablock
      );
      set @end_time = GETDATE();
      PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '------------------------------------------';

      -- Load BRONZE.erp_loc_a101
      set @start_time = GETDATE();
      PRINT ' >> Truncating table BRONZE.erp_loc_a101 ';
      TRUNCATE TABLE BRONZE.erp_loc_a101;

      PRINT ' >> Inserting Data : BRONZE.erp_loc_a101 ';
      BULK INSERT BRONZE.erp_loc_a101
      FROM 'E:\Data_SQL\Project_files\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
      WITH (
          FIELDTERMINATOR = ',',
          FIRSTROW = 2,
          tablock
      );
      set @end_time = GETDATE();
      PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '------------------------------------------';

      -- Load BRONZE.erp_px_cat_g1v2
      set @start_time = GETDATE();
      PRINT ' >> Truncating table BRONZE.erp_px_cat_g1v2 ';
      TRUNCATE TABLE BRONZE.erp_px_cat_g1v2;

      PRINT ' >> Inserting Data : BRONZE.erp_px_cat_g1v2 ';
      BULK INSERT BRONZE.erp_px_cat_g1v2
      FROM 'E:\Data_SQL\Project_files\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
      WITH (
          FIELDTERMINATOR = ',',
          FIRSTROW = 2,
          tablock
      );
      set @end_time = GETDATE();
      PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '------------------------------------------';

      set @overall_end = GETDATE();
      PRINT '=======================================';
      PRINT 'Total ERP load duration: ' + CAST(DATEDIFF(SECOND, @overall_start, @overall_end) AS NVARCHAR(10)) + ' seconds';
      PRINT '=======================================';

   END TRY
   BEGIN CATCH
      PRINT '=======================================';
      PRINT 'Error occurred while loading ERP data';
      PRINT '=======================================';
      PRINT 'Error Message: ' + ERROR_MESSAGE();
      PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
   END CATCH
END