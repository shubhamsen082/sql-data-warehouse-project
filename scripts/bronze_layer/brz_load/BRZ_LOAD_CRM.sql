CREATE OR ALTER PROCEDURE bronze.load_bronze_crm AS
BEGIN
 DECLARE @start_time DATETIME, @end_time DATETIME;
 DECLARE @overall_start DATETIME, @overall_end DATETIME;
 BEGIN TRY
   PRINT '======================================';
   PRINT 'Loading CRM data into Bronze layer';
   PRINT '======================================';
   ----- FULL LOAD SQL SCRIPT -----
   set @overall_start = GETDATE();

   set @start_time = GETDATE();
   PRINT ' >> Truncating table bronze.crm_cust_info ';
   TRUNCATE TABLE bronze.crm_cust_info;

   PRINT ' >> Inserting Data : bronze.crm_cust_info ';
   BULK INSERT bronze.crm_cust_info
   FROM 'E:\Data_SQL\Project_files\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
      FIELDTERMINATOR = ',',
      FIRSTROW = 2,
      tablock
    );
   set @end_time = GETDATE();
   PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
   PRINT '------------------------------------------';

   set @start_time = GETDATE();
   PRINT ' >> Truncating table bronze.crm_prd_info ';
   TRUNCATE TABLE bronze.crm_prd_info;

   PRINT ' >> Inserting Data : bronze.crm_prd_info ';
   BULK INSERT bronze.crm_prd_info
   FROM 'E:\Data_SQL\Project_files\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
      WITH (  
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        tablock
           );
   set @end_time = GETDATE();
   PRINT ' >> load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
   PRINT '------------------------------------------';

   set @start_time = GETDATE();
   PRINT ' >> Truncating table bronze.crm_sales_details ';
   TRUNCATE TABLE bronze.crm_sales_details;

   PRINT ' >> Inserting Data : bronze.crm_sales_details ';
   BULK INSERT bronze.crm_sales_details
   FROM 'E:\Data_SQL\Project_files\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
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
   PRINT 'Total CRM load duration: ' + CAST(DATEDIFF(SECOND, @overall_start, @overall_end) AS NVARCHAR(10)) + ' seconds';
   PRINT '=======================================';
 END TRY
 BEGIN CATCH
   PRINT '=======================================';
   PRINT 'Error occurred while loading CRM data';
   PRINT '=======================================';
   PRINT 'Error Message: ' + ERROR_MESSAGE();
   PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
 END CATCH
END 