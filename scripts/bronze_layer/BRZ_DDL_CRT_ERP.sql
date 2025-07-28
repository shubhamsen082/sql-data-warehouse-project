IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
 cid NVARCHAR(50),
 bdate DATE,
 gen NVARCHAR(50)
)

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
 id NVARCHAR(50),
 cat NVARCHAR(50),
 subcat NVARCHAR(50),
 maintenance NVARCHAR(10)
)

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
 cid NVARCHAR(50),
 cntry NVARCHAR(50)
)