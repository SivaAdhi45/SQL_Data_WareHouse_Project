/*
===============================================================================
Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/

-- Cust_info table data cleaning and insertion into silver schema

insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)

Select cst_id,
	   trim(cst_key),
	   trim(cst_firstname),
	   trim(cst_lastname),
	   case 
			when upper(cst_marital_status) = 'M' then 'Married'
			when upper(cst_marital_status) = 'S' then 'Single'
			Else 'n/a'
	   End as cst_marital_status,
	   case 
			when upper(cst_gndr) = 'M' then 'Male'
			when upper(cst_gndr) = 'F' then 'Female'
			Else 'n/a'
	   End as cst_gndr,
	   cst_create_date
from
	(Select 
		*,
		 row_number() over(partition by cst_id order by cst_create_date) as Flags_last
	 from bronze.crm_cust_info
	 where cst_id <> '0') as cst_id
where flags_last =1;

-- Cst_info table - Clean Data Validation

-- Validating: Primary Key should not have any duplicates or null values
-- Expectations: None

Select cst_id,count(*) from silver.crm_cust_info
group by cst_id
having count(*)>1;

-- Validation: Data should not have any unnecessary spaces.
-- Expected result: None

Select cst_firstname from silver.crm_cust_info
where cst_firstname<>trim(cst_firstname);

Select cst_lastname from silver.crm_cust_info
where cst_lastname<>trim(cst_lastname);

-- validation: Data should have meaningful information (No abbrevations)

Select distinct cst_marital_status from silver.crm_cust_info;

Select distinct cst_gndr from silver.crm_cust_info;

-- prd_info table data cleaning and insertion into silver schema

Select prd_id,
       replace(substring(prd_key,1,5),'-','_') as cat_id,
       substring(prd_key,7,length(prd_key)) as prd_key,
       prd_nm,
       prd_cost,
       case upper(trim(prd_line))
			when 'M' then 'Mountain'
            when 'S' then 'Other Sales'
            when 'R' then 'Road'
            when 'T' then 'Touring'
            Else 'n/a'
		End as prd_line,
        prd_start_dt,
        lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)- interval 1 day as prd_end_dt
from bronze.crm_prd_info;

drop table if exists silver.crm_prd_info;
create table silver.crm_prd_info
	(
	 prd_id int,
     cat_id varchar(10),
	 prd_key varchar(35),
	 prd_nm varchar(75),
	 prd_cost int,
	 prd_line varchar(25),
	 prd_start_dt date,
	 prd_end_dt date,
     dwh_create_date timestamp default current_timestamp
	);
    
Insert into silver.crm_prd_info (prd_id, cat_id ,prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
Select trim(prd_id),
       trim(replace(substring(prd_key,1,5),'-','_')) as cat_id,
       trim(substring(prd_key,7,length(prd_key))) as prd_key,
       trim(prd_nm),
       prd_cost,
       case upper(trim(prd_line))
			when 'M' then 'Mountain'
            when 'S' then 'Other Sales'
            when 'R' then 'Road'
            when 'T' then 'Touring'
            Else 'n/a'
		End as prd_line,
        prd_start_dt,
        lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)- interval 1 day as prd_end_dt
from bronze.crm_prd_info;

Select * from silver.crm_prd_info;

-- Validating: Primary Key should not have any duplicates or null values
-- Expectations: No results

Select prd_id,count(*) from silver.crm_prd_info
group by prd_id
having count(*)>1;

-- Validation: Data should not have any unnecessary spaces.
-- Expected result: None

Select cat_id
from silver.crm_prd_info
having cat_id<>trim(cat_id);

Select prd_key
from silver.crm_prd_info
having prd_key<>trim(prd_key);

Select prd_nm
from silver.crm_prd_info
having prd_nm<>trim(prd_nm);

-- Check: Null and negative values in cost column

select prd_cost from silver.crm_prd_info
where prd_cost is null or prd_cost < 0;

Select prd_start_dt from silver.crm_prd_info
where prd_start_dt is null or prd_start_dt > curdate();


-- crm_sales_details table data cleaning and insertion into silver schema

insert into silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
Select 
		trim(sls_ord_num),
        trim(sls_prd_key),
        trim(sls_cust_id),
        case 
			when sls_order_dt = 0 then null
            else sls_order_dt
            end as sls_order_dt,
        case 
			when sls_ship_dt = 0 then null
            else sls_ship_dt
            end as sls_ship_dt,
        case 
			when sls_due_dt = 0 then null
            else sls_due_dt
            end as sls_due_dt,
		case 
			when sls_sales <= 0 or sls_sales is null  then abs(ifnull(sls_price,0)) * sls_quantity
            when sls_price <> 0 and sls_sales <> abs(sls_price) * sls_quantity then abs(sls_price) * sls_quantity
            else abs(sls_sales)
            end as sls_sales,
		sls_quantity,
		case 
			when sls_price <= 0 or sls_price is null then abs(sls_sales) / nullif(sls_quantity,0)
            Else abs(sls_price)
            end as sls_price
from bronze.crm_sales_details;

Select * from silver.crm_sales_details;

-- Sales_details column Data quality check:

-- Check for null values
-- Expectations: no results

Select * from silver.crm_sales_details
where sls_ord_num is null;

Select * from silver.crm_sales_details
where sls_prd_key is null;

Select * from silver.crm_sales_details
where sls_cust_id is null;

-- Check for unnecessary spaces
-- Expectations: no results

Select sls_ord_num from silver.crm_sales_details
where sls_ord_num <> trim(sls_ord_num);

Select sls_prd_key from silver.crm_sales_details
where sls_prd_key <> trim(sls_prd_key);

-- Check for additional values in foriegn key which are not in primary key:

Select sls_prd_key from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

-- Check for invalid dates and date rules for date columns

Select sls_order_dt,sls_ship_dt,sls_due_dt
from silver.crm_sales_details
where sls_order_dt <= 0 or sls_ship_dt <= 0 or sls_due_dt <= 0;

Select sls_order_dt,sls_ship_dt,sls_due_dt
from silver.crm_sales_details
where sls_order_dt > curdate() or sls_ship_dt > curdate() or sls_due_dt > curdate()
or sls_order_dt < '1990-01-01' or sls_ship_dt < '1990-01-01' or sls_due_dt < '1990-01-01';

Select sls_order_dt,sls_ship_dt,sls_due_dt
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt or sls_ship_dt >  sls_due_dt;

Select sls_order_dt,sls_ship_dt,sls_due_dt
from silver.crm_sales_details
where length(sls_order_dt) <>10 or length(sls_ship_dt) <>10 or length(sls_due_dt) <>10;

-- Check for negative values, zeros, null values for sales, qualtity and price

Select sls_sales,sls_quantity,sls_price
from silver.crm_sales_details
where sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_price * sls_quantity <> sls_sales
order by sls_sales,sls_quantity,sls_price;

-- erp_cust_az12 table data cleaning and insertion into silver schema

Select * from bronze.erp_cust_az12;

insert into silver.erp_cust_az12 (CID, BDATE, GEN)
Select
	case 
		when cid like 'NAS%' then substr(cid,4,length(cid))
        else cid
        end as CID,
	case 
		when bdate > curdate() then null
        else bdate
        end as bdate,
	case 
		when trim(gen) = 'Male' or trim(gen) = 'M' then 'Male'
        when trim(gen) = 'Female' or trim(gen) = 'F' then 'Female'
        else 'n/a'
        End as gen
from bronze.erp_cust_az12;

Select * from silver.erp_cust_az12;

-- erp_cust_az12 column Data quality check:

-- Identify out of range Dates

Select bdate from silver.erp_cust_az12
where bdate > curdate() or bdate < '1900-01-01'
order by bdate;

-- Data Standardization and consistency

Select distinct gen from silver.erp_cust_az12;

-- Additional values in foriegn key

Select cid from silver.erp_cust_az12
where cid not in (Select distinct cst_key from silver.crm_cust_info)

-- erp_loc_a101 table data cleaning and insertion into silver schema

truncate table silver.erp_loc_a101;
Insert into silver.erp_loc_a101 (CID, cntry)
Select 
	replace(cid,'-',''),
    case trim(cntry)
		when 'DE' then 'Germany'
        when 'US' then 'United States'
        when 'USA' then 'United States'
        when '' then 'n/a'
        Else trim(cntry)
        End as cntry
from bronze.erp_loc_a101;
	
-- erp_loc_a101 column Data quality check:

-- Data Standardization and consistency

Select distinct cntry from silver.erp_loc_a101;

-- Additional values in foriegn key

Select cid from silver.erp_loc_a101
where cid not in (Select distinct cst_key from silver.crm_cust_info);

-- erp_px_cat_g1v2 table data cleaning and insertion into silver schema

insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
Select 
	trim(id),
    trim(cat),
    trim(subcat),
    trim(maintenance)
from bronze.erp_px_cat_g1v2;

-- erp_px_cat_g1v2 column Data quality check:

-- check for unwanted spaces:

Select * from silver.erp_px_cat_g1v2
where id <> trim(id) or cat <> trim(cat) or subcat <> trim(subcat) or maintenance <> trim(maintenance);

-- Additional values in foriegn key:

Select distinct cat_id from silver.crm_prd_info
where cat_id not in (select id from silver.erp_px_cat_g1v2);

-- Data standardization and Normalization:

Select distinct cat from silver.erp_px_cat_g1v2;
Select distinct subcat from silver.erp_px_cat_g1v2;
Select distinct maintenance from silver.erp_px_cat_g1v2;

-- Data Standardization and normalization between primary and foriegn key

Select distinct cat_id from silver.crm_prd_info
where cat_id not in (select id from silver.erp_px_cat_g1v2);

update silver.crm_prd_info 
set cat_id='CO_PD'
where cat_id='CO_PE';

