---SELECT * FROM bronze.crm_cust_info;

select top 1000 * from bronze.crm_prd_info;

----select top 1000 * from bronze.crm_sales_details;

----select * from bronze.erp_cust_az12;

SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT cst_material_status
FROM SILVER.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM SILVER.crm_cust_info;

select * from silver.crm_cust_info
WHERE cst_ID IS NULL;

SELECT
cst_id,
COUNT(*)
from silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--- check for unwanted spaces
SELECT cst_first_name,cst_last_name,cst_gndr from silver.crm_cust_info
where cst_first_name != TRIM(cst_first_name)
OR cst_last_name != TRIM(cst_last_name)
OR cst_gndr!= TRIM(cst_gndr);

select * from silver.crm_cust_info;

SELECT * from silver.erp_cust_az12;

EXEC silver.load_silver_crm;





