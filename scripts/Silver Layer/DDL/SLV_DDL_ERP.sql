IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
 cid NVARCHAR(50),
 bdate DATE,
 gen NVARCHAR(50),
 dwh_created_date DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
 id NVARCHAR(50),
 cat NVARCHAR(50),
 subcat NVARCHAR(50),
 maintenance NVARCHAR(10),
 dwh_created_date DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
 cid NVARCHAR(50),
 cntry NVARCHAR(50),
 dwh_created_date DATETIME DEFAULT GETDATE()
)