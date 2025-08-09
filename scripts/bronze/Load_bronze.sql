/*
===============================================================================
Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA` command to load data from csv Files to bronze tables.
===============================================================================
*/

-- Inserting Data into the bronze schema tables

-- Adding crm_cust_info table data

Truncate table bronze.crm_cust_info;
Load data local infile '"C:\\SQL Projects Folder\\Data Engineering Project P3\\sql-data-warehouse-project\\datasets\\source_crm\\cust_info.csv"'
into table bronze.crm_cust_info
Fields terminated by ','
Lines terminated by '\r\n'
ignore 1 lines;

-- Adding crm_prd_info table data

Truncate table bronze.crm_prd_info;
Load data local infile 'C:\\SQL Projects Folder\\Data Engineering Project P3\\sql-data-warehouse-project\\datasets\\source_crm\\prd_info.csv'
into table bronze.crm_prd_info
Fields terminated by ','
Lines terminated by '\r\n'
ignore 1 lines;

-- Adding crm_sales_details table data

Truncate table bronze.crm_sales_details;
Load data local infile 'C:\\SQL Projects Folder\\Data Engineering Project P3\\sql-data-warehouse-project\\datasets\\source_crm\\sales_details.csv'
into table bronze.crm_sales_details
Fields terminated by ','
Lines terminated by '\r\n'
ignore 1 lines;

-- Adding erp_cust_az12 table data

truncate table bronze.erp_cust_az12;
load data local infile 'C:\\SQL Projects Folder\\Data Engineering Project P3\\sql-data-warehouse-project\\datasets\\source_erp\\CUST_AZ12.csv'
into table bronze.erp_cust_az12
fields terminated by ','
lines terminated by '\r\n'
ignore 1 lines;

-- Adding erp_loc_a101 table details

Truncate table bronze.erp_loc_a101;
load data local infile 'C:\\SQL Projects Folder\\Data Engineering Project P3\\sql-data-warehouse-project\\datasets\\source_erp\\LOC_A101.csv'
into table bronze.erp_loc_a101
fields terminated by ','
lines terminated by '\r\n'
ignore 1 lines;

-- Adding erp_px_cat_g1v2 table details

truncate table bronze.erp_px_cat_g1v2;
load data local infile 'C:\\\SQL Projects Folder\\Data Engineering Project P3\\sql-data-warehouse-project\\datasets\\source_erp\\PX_CAT_G1V2.csv'
into table bronze.erp_px_cat_g1v2
fields terminated by ','
lines terminated by '\r\n'
ignore 1 lines;

Select * from bronze.crm_prd_info;
select count(*) from bronze.crm_prd_info;

Select * from bronze.crm_cust_info;
select count(*) from bronze.crm_prd_info;

select * from bronze.crm_sales_details;
select count(*) from bronze.crm_sales_details;

select * from bronze.erp_cust_az12;
select count(*) from bronze.erp_cust_az12;

select * from bronze.erp_loc_a101;
Select count(*) from bronze.erp_loc_a101;

Select * from bronze.erp_px_cat_g1v2;
Select count(*) from bronze.erp_px_cat_g1v2;
