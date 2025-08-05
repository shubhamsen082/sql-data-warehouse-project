---Check for Nulls or Duplicates
-- Expectations No results
SELECT
cst_id,
COUNT(*)
from bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--- check for unwanted spaces
SELECT cst_first_name,cst_last_name,cst_gndr from bronze.crm_cust_info
where cst_first_name != TRIM(cst_first_name)
OR cst_last_name != TRIM(cst_last_name)
OR cst_gndr != TRIM(cst_gndr);

------Data standardization and inconsistency checks
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

select * from bronze.crm_cust_info
WHERE cst_ID IS NULL;

SELECT distinct prd_line 
from bronze.crm_prd_info;

-----------------Check for Nulls or Duplicates in Product Info
SELECT prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--- check for unwanted spaces
SELECT prd_nm from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm);

---- check for NULLS or negative values in product cost
SELECT prd_cost from bronze.crm_prd_info
where prd_cost IS NULL OR prd_cost < 0;

---- check for invalid date order
SELECT prd_start_dt, prd_end_dt from bronze.crm_prd_info
where prd_start_dt > prd_end_dt;

SELECT prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--- check for unwanted spaces
SELECT prd_nm from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

---- check for NULLS or negative values in product cost
SELECT prd_cost from silver.crm_prd_info
where prd_cost IS NULL OR prd_cost < 0;

---- check for invalid date order
SELECT prd_start_dt, prd_end_dt from silver.crm_prd_info
where prd_start_dt > prd_end_dt;


---- check for invalid dates
SELECT nullif(sls_order_dt,0)
from bronze.crm_sales_details
where 
sls_order_dt <= 0 or 
len(sls_order_dt)!=8 OR
sls_order_dt > 20500101
or sls_order_dt < 19000101; --- 

SELECT nullif(sls_ship_dt,0)
from bronze.crm_sales_details
where 
sls_ship_dt <= 0 or 
len(sls_ship_dt)!=8 OR
sls_ship_dt > 20500101
or sls_ship_dt < 19000101; 

SELECT
sls_sales as old_sls_sales,
sls_quantity as old_sls_quantity,
sls_quantity,
CASE when sls_sales IS NULL or sls_sales <=0 or (sls_sales != sls_quantity * abs(sls_price))
     THEN sls_quantity * abs(sls_price)
     else sls_sales
     END as sls_sales,
CASE when sls_price is NULL or sls_price <=0
     THEN sls_sales/NULLIF(sls_quantity,0)
     else sls_price
     END as sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL or sls_quantity IS NULL or sls_price IS NULL
OR sls_sales <=0 or sls_quantity <= 0 or sls_price <=0;

--- Data standiraztion and consistency
SELECT
cid,
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
from silver.erp_cust_az12;

select distinct
bdate from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE();


select distinct cntry
from bronze.erp_loc_a101
order by cntry;


-------check unwanted spaces

select * FROM bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat)
or maintenance != TRIM(maintenance);

--- Data standiraztion and consistency
SELECT distinct cat
from bronze.erp_px_cat_g1v2;

SELECT distinct subcat
from bronze.erp_px_cat_g1v2;

SELECT distinct maintenance
from bronze.erp_px_cat_g1v2;