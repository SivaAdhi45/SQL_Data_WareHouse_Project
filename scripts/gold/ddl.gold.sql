/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

Create or replace view Gold.Dim_Customers as
Select 
  	row_number() over (order by ci.cst_id) as Customer_Key,
  	ci.cst_id as Customer_ID,
  	ci.cst_key as Customer_Number,
  	ci.cst_firstname as First_Name,
  	ci.cst_lastname as Last_name,
    bd.BDATE as Birth_Date,
    case 
  		when ci.cst_gndr <>'n/a' then ci.cst_gndr -- CRM is the master data for gender info.
  		else coalesce(bd.gen,'n/a')
  	end as Gender,
  	ci.cst_marital_status as Marital_Status,
  	lo.CNTRY as Country,
  	ci.cst_create_date as Create_Date
from silver.crm_cust_info ci
	left join silver.erp_cust_az12 bd
	on ci.cst_key = bd.cid
		left join silver.erp_loc_a101 lo
		on ci.cst_key = lo.cid;
        
Select * from gold.dim_customers;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

Create view Gold.dim_products as
	Select
		row_number() over (order by prd_start_dt,prd_id) as Product_Key,
		pd.prd_id as Product_ID,
		pd.prd_key as Product_Number,
		pd.prd_nm as Product_Name,
		pd.cat_id as Category_id,
		ct.cat as Category,
		ct.SUBCAT as Subcategory,
		pd.prd_line as Product_line,
		ct.MAINTENANCE as Maintenace,
		pd.prd_cost as Cost,
		pd.prd_start_dt as Start_Date
	from silver.crm_prd_info pd
	left join silver.erp_px_cat_g1v2 ct
	on pd.cat_id = ct.id
	where pd.prd_end_dt is null; -- filter out all the historical data.

Select * from gold.dim_products;

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

Create view Gold.fact_sales as
	Select
		sl.sls_ord_num as Order_Number,
		pd.Product_Key,
		ci.Customer_Key,
		sl.sls_order_dt as Order_Date,
		sl.sls_ship_dt as Shipping_Date,
		sl.sls_due_dt as Due_Date,
		sl.sls_sales as Sales_Amount,
		sl.sls_quantity as Quantity,
		sl.sls_price as Price
	from silver.crm_sales_details sl
	left join gold.dim_customers ci
	on sl.sls_cust_id = ci.customer_id
	left join gold.dim_products pd
	on sl.sls_prd_key=pd.product_number;
    
Select * from gold.fact_sales;
