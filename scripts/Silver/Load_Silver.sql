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

-- Loading Silver.crm.cust_info

Insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)

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

--  Loading Silver.crm.prd_info

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

-- Loading Silver.crm_sales_details

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

-- Loading Silver.erp_cust_az12

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

-- Loading Silver.erp_loc_a101

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

-- Loading Silver.erp_px_cat_g1v2

insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
Select 
	trim(id),
    trim(cat),
    trim(subcat),
    trim(maintenance)
from bronze.erp_px_cat_g1v2;

-- Data Standardization and normalization between primary and foriegn key

Select distinct cat_id from silver.crm_prd_info
where cat_id not in (select id from silver.erp_px_cat_g1v2);

update silver.crm_prd_info 
set cat_id='CO_PD'
where cat_id='CO_PE';

