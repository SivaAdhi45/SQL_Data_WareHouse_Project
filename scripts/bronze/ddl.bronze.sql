/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Bronze layer: Table Creation

Create table bronze.crm_cust_info 
	( 
		cst_id int,
		cst_key varchar(15),
		cst_firstname varchar(50),
		cst_lastname varchar(50),
		cst_marital_status varchar(10),
		cst_gndr varchar(10),
		cst_create_date date
	);

Select * from bronze.crm_cust_info;

create table bronze.crm_prd_info
	(
	 prd_id int,
	 prd_key varchar(35),
	 prd_nm varchar(75),
	 prd_cost int,
	 prd_line varchar(10),
	 prd_start_dt date,
	 prd_end_dt date
	);
    
select * from bronze.crm_prd_info;

Create table bronze.crm_sales_details
	(
	  sls_ord_num varchar(15),
      sls_prd_key varchar(35),
      sls_cust_id int,
      sls_order_dt date,
      sls_ship_dt date,
      sls_due_dt date,
      sls_sales	int,
      sls_quantity int,
      sls_price int
	);
    
Select * from bronze.crm_sales_details;

Create table bronze.erp_cust_az12
	(
      CID varchar(20),
      BDATE date,
      GEN varchar(10)
	);

select * from bronze.erp_cust_az12;

create table bronze.erp_loc_a101
	(
      CID varchar(20),
      CNTRY varchar(75)
	);
    
Create table bronze.erp_px_cat_g1v2
	(
      ID varchar(10),
      CAT varchar(30),
      SUBCAT varchar(50),
      MAINTENANCE varchar(10)
	);
