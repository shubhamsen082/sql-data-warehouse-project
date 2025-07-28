CREATE TABLE bronze.crm_cust_info (
    cst_id INT PRIMARY KEY,
    cst_key NVARCHAR(50),
    cst_first_name NVARCHAR(50),
    cst_last_name NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(10),
    cst_created_date DATE
);