With initial_pull as (
select 
  rs.cstm_kdnr
, pa.prtn_name as cstm_name

, pa.oprt_bed
, concat(op1.oprt_last_name,', ',op1.oprt_first_name) as kdnr_account_owner
, pa.prtn_account_owner_type as kdnr_sf_account_owner_type
, pa.prtn_person_region as kdnr_prtn_person_region --Team level
, pa.prtn_working_channel as kdnr_prtn_working_channel --Sales Region

, pa.oprt_bed_vkni as kdnr_bed_vkni --Inside Sales
, concat(op4.oprt_last_name,', ',op4.oprt_first_name) as kdnr_inside_sales
, pa.prtn_inside_sales_person_region as kdnr_sf_inside_sales_person_region
, pa.prtn_inside_sales_working_channel as kdnr_sf_inside_sales_working_channel

, pa.prtn_acquired_by_rent_id as kdnr_sf_acquired_by_rent_id
, concat(op7.oprt_last_name,', ',op7.oprt_first_name) as kdnr_acquired_by
, pa.prtn_acquired_by_rent_region as kdnr_sf_acquired_by_rent_region
, pa.prtn_acquired_by_rent_saleschannel as kdnr_sf_acquired_by_rent_saleschannel

, pa.prtn_subsidiary_calc_num as dto
, pa.prtn_subsidiary_calc_name as dto_name

, dto.oprt_bed
, concat(op2.oprt_last_name,', ',op2.oprt_first_name) as dto_account_owner
, dto.prtn_account_owner_type as dto_sf_account_owner_type
, dto.prtn_person_region as dto_prtn_person_region --Team level
, dto.prtn_working_channel as dto_prtn_working_channel --Sales Region

, dto.oprt_bed_vkni as dto_bed_vkni --Inside Sales
, concat(op5.oprt_last_name,', ',op5.oprt_first_name) as dto_inside_sales
, dto.prtn_inside_sales_person_region as dto_sf_inside_sales_person_region
, dto.prtn_inside_sales_working_channel as dto_sf_inside_sales_working_channel

, dto.prtn_acquired_by_rent_id as dto_sf_acquired_by_rent_id
, concat(op8.oprt_last_name,', ',op8.oprt_first_name) as dto_acquired_by
, dto.prtn_acquired_by_rent_region as dto_sf_acquired_by_rent_region
, dto.prtn_acquired_by_rent_saleschannel as dto_sf_acquired_by_rent_saleschannel



, pa.prtn_parent_calc_num as dfi
, pa.prtn_parent_calc_name as dfi_name

, dfi.oprt_bed
, concat(op3.oprt_last_name,', ',op3.oprt_first_name) as dfi_account_owner
, dfi.prtn_account_owner_type as dfi_sf_account_owner_type
, dfi.prtn_person_region as dfi_prtn_person_region --Team level
, dfi.prtn_working_channel as dfi_prtn_working_channel --Sales Region

, dfi.oprt_bed_vkni as dfi_bed_vkni --Inside Sales
, concat(op6.oprt_last_name,', ',op6.oprt_first_name) as dfi_inside_sales
, dfi.prtn_inside_sales_person_region as dfi_sf_inside_sales_person_region
, dfi.prtn_inside_sales_working_channel as dfi_sf_inside_sales_working_channel

, dfi.prtn_acquired_by_rent_id as dfi_sf_acquired_by_rent_id
, concat(op9.oprt_last_name,', ',op9.oprt_first_name) as dfi_acquired_by
, dfi.prtn_acquired_by_rent_region as dfi_sf_acquired_by_rent_region
, dfi.prtn_acquired_by_rent_saleschannel as dfi_sf_acquired_by_rent_saleschannel

, sum(rs.rntl_revenue) as rntl_revenue

from "rent_shop"."ra_fct_rental_series" rs
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = rs.cstm_kdnr
left join "customer_shop"."pa_dim_partners" dto on dto.prtn_kdnr = pa.prtn_subsidiary_calc_num
left join "customer_shop"."pa_dim_partners" dfi on dfi.prtn_kdnr = pa.prtn_parent_calc_num
left join "hr_shop"."op_dim_operators" op1 on op1.oprt_bed = pa.oprt_bed
left join "hr_shop"."op_dim_operators" op2 on op2.oprt_bed = dto.oprt_bed
left join "hr_shop"."op_dim_operators" op3 on op3.oprt_bed = dfi.oprt_bed
left join "hr_shop"."op_dim_operators" op4 on op4.oprt_bed = pa.oprt_bed_vkni
left join "hr_shop"."op_dim_operators" op5 on op5.oprt_bed = dto.oprt_bed_vkni
left join "hr_shop"."op_dim_operators" op6 on op6.oprt_bed = dfi.oprt_bed_vkni
left join "hr_shop"."op_dim_operators" op7 on cast(op7.oprt_bed as varchar) = pa.prtn_acquired_by_rent_id
left join "hr_shop"."op_dim_operators" op8 on cast(op8.oprt_bed as varchar) = dto.prtn_acquired_by_rent_id
left join "hr_shop"."op_dim_operators" op9 on cast(op9.oprt_bed as varchar) = dfi.prtn_acquired_by_rent_id
where pa.prtn_parent_calc_num = 71587 and year (rs.rntl_accounting_date) = 2023
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45)

select * from initial_pull 

select * from "rent_shop"."ra_fct_rental_series" where year (rntl_accounting_date) = 2023 limit 1000 and cstm_kdnr = 15


, aggregated as (
select 
  dfi
, count(distinct cstm_kdnr) as kdnrs
, count(distinct dto) as dtos 
, count(distinct kdnr_sf_acquired_by_rent_id)+count(distinct dto_sf_acquired_by_rent_id)+count(distinct kdnr_sf_acquired_by_rent_id) as acquired_ids
from initial_pull 
group by 1)


select * from aggregated where dtos between 3 and 5 and kdnrs between 8 and 12 

SELECT * FROM "sds_prod_ingestion_store_public_datalake"."customerorgv1" where "company_data.customer_number" = 15 order by "meta.timestamp" desc
SELECT * FROM "sds_prod_ingestion_store_public_datalake"."salespersonworkingchannel" limit 10;
select * from "customer_shop"."pa_dim_partners" limit 100
select * from "hr_shop"."op_dim_operators" limit 100


