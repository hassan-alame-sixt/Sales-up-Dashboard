/*START: Specifying table destination*/
create table if not exists "sales_mart"."sales_up_all_table"
with (format='Parquet',
external_location='s3://sds-prod-store-marts/sales_mart/sales_up_all_table',
parquet_compression = 'SNAPPY') as
--Table Granularity is rs.rntl_mvnr, rs.rntl_mser, rs.rntl_konr, ch.chra_pos, ch.chra_inty + re.rsrv_resn
/*END: Specifying table destination*/

with initial_pull as (
select 
  sm.rntl_mvnr
, sm.rntl_mser
, sm.brnc_name
, sm.brnc_main_type
, sm.brnc_type
, sm.brnc_country_code_iso
, sm.brnc_country
, sm.brnc_corporate_franchise
, sm.brnc_country_region
, sm.brnc_country_region_franchise_breakdown
, sm.cstm_kdnr
, sm.prtn_name
, sm.abkz
, sm.prtn_parent_domestic_country
, sm.cleansed_dfr
, sm.kdnr_oprt_bed
, sm.kdnr_account_owner
, sm.kdnr_account_owner_type
, sm.kdnr_account_owner_region
, sm.kdnr_owner_segment_mapping
, sm.kdnr_owner_responsiblity_mapping
, sm.kdnr_account_owner_working_channel
, sm.oprt_kdnr_dto_duplicate
, sm.oprt_kdnr_dfi_duplicate
, sm.oprt_region_kdnr_dto_duplicate
, sm.oprt_region_kdnr_dfi_duplicate
, sm.kdnr_bed_vkni
, sm.kdnr_inside_sales
, sm.kdnr_inside_sales_type
, sm.kdnr_inside_sales_region
, sm.kdnr_inside_segment_mapping
, sm.kdnr_inside_responsiblity_mapping
, sm.kdnr_inside_sales_working_channel
, sm.vkni_kdnr_dto_duplicate
, sm.vkni_kdnr_dfi_duplicate
, sm.vkni_region_kdnr_dto_duplicate
, sm.vkni_region_kdnr_dfi_duplicate
, sm.kdnr_acquired_by_bed
, sm.kdnr_acquired_by
, sm.kdnr_acquired_by_type
, sm.kdnr_acquired_by_region
, sm.kdnr_acquired_segment_mapping
, sm.kdnr_acquired_responsiblity_mapping
, sm.kdnr_acquired_by_working_channel
, sm.acquired_by_kdnr_dto_duplicate
, sm.acquired_by_kdnr_dfi_duplicate
, sm.acquired_region_kdnr_dto_duplicate
, sm.acquired_region_kdnr_dfi_duplicate
, sm.kdnr_acquired_rent_date
, sm.sf_deal_amount
, sm.sf_total_sixt_potential
, sm.sf_total_customer_spend
, sm.dto
, sm.dto_name
, sm.dto_oprt_bed
, sm.dto_account_owner
, sm.dto_account_owner_type
, sm.dto_account_owner_region
, sm.dto_owner_segment_mapping
, sm.dto_owner_responsiblity_mapping
, sm.dto_account_owner_working_channel
, sm.oprt_dto_dfi_duplicate
, sm.oprt_region_dto_dfi_duplicate
, sm.dto_bed_vkni
, sm.dto_inside_sales
, sm.dto_inside_sales_type
, sm.dto_inside_sales_region
, sm.dto_inside_segment_mapping
, sm.dto_inside_responsiblity_mapping
, sm.dto_inside_sales_working_channel
, sm.vkni_dto_dfi_duplicate
, sm.vkni_region_dto_dfi_duplicate
, sm.dto_acquired_by_bed
, sm.dto_acquired_by
, sm.dto_acquired_by_type
, sm.dto_acquired_by_region
, sm.dto_acquired_segment_mapping
, sm.dto_acquired_responsiblity_mapping
, sm.dto_acquired_by_working_channel
, sm.acquired_by_dto_dfi_duplicate
, sm.acquired_by_region_dto_dfi_duplicate
, sm.dfi
, sm.dfi_name
, sm.dfi_oprt_bed
, sm.dfi_account_owner
, sm.dfi_account_owner_type
, sm.dfi_account_owner_region
, sm.dfi_owner_segment_mapping
, sm.dfi_owner_responsiblity_mapping
, sm.dfi_account_owner_working_channel
, sm.dfi_bed_vkni
, sm.dfi_inside_sales
, sm.dfi_inside_sales_type
, sm.dfi_inside_sales_region
, sm.dfi_inside_segment_mapping
, sm.dfi_inside_responsiblity_mapping
, sm.dfi_inside_sales_working_channel
, sm.dfi_acquired_by_bed
, sm.dfi_acquired_by
, sm.dfi_acquired_by_type
, sm.dfi_acquired_by_region
, sm.dfi_acquired_segment_mapping
, sm.dfi_acquired_responsiblity_mapping
, sm.dfi_acquired_by_working_channel

, sm.app_reservation_resn
, sm.rsrv_cancelled_resn
, sm.rsrv_noshow_resn
, sm.internet_reservation_mvnr
, sm.app_reservation_mvnr
, sm.rntl_one_way_mvnr
, sm.rntl_correction_mvnr
, sm.domestic_country_mvnr
, sm.rntl_bev_mvnr
, sm.cstm_account_manager_num
, sm.cstm_account_manager_name
, sm.rntl_accounting_date
, sm.vhcl_group
, sm.vhcl_checked_out_group
, sm.vhcl_type
, sm.vhat_elty
, sm.AKTV

, sm.product_level1_source
, sm.product_level2_car_truck
, sm.product_level2_cnb_vnt
, sm.product_level3_name
, sm.product

, sm.rsts_excitement_num
, sm.rsts_recommendation_num
, sm.rntl_revenue
, sm.rntl_discount
, sm.rental_days
, sm.advanced_booking

, sm.internet_reservation_revenue
, sm.app_reservation_revenue
, sm.domestic_revenue
, sm.non_domestic_revenue
from "sales_mart"."sales_up_main_table" sm
where sm.rntl_mvnr is not null --To eliminate resrvations with no rentals
)

, unpivotted_ownership as (
select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'KDNR' as hierarchy_level
, ip.cstm_kdnr as account_number
, ip.prtn_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Account Owner' as ownership_level
, cast(ip.kdnr_oprt_bed as varchar) as owner_bed
, ip.kdnr_account_owner as owner_name
, ip.kdnr_account_owner_type as owner_type
, ip.kdnr_account_owner_region as owner_region
, ip.kdnr_owner_segment_mapping as owner_segment_mapping
, ip.kdnr_owner_responsiblity_mapping as owner_responsibility_mapping
, ip.kdnr_account_owner_working_channel as owner_working_channel
, ip.oprt_region_kdnr_dto_duplicate as region_dto_duplicate
, ip.oprt_region_kdnr_dfi_duplicate as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip
where ip.oprt_kdnr_dto_duplicate = false and ip.oprt_kdnr_dfi_duplicate = false

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'DTO' as hierarchy_level
, ip.dto as account_number
, ip.dto_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Account Owner' as ownership_level
, cast(ip.dto_oprt_bed as varchar) as owner_bed
, ip.dto_account_owner as owner_name
, ip.dto_account_owner_type as owner_type
, ip.dto_account_owner_region as owner_region
, ip.dto_owner_segment_mapping as owner_segment_mapping
, ip.dto_owner_responsiblity_mapping as owner_responsibility_mapping
, ip.dto_account_owner_working_channel as owner_working_channel
, false as region_dto_duplicate
, ip.oprt_region_dto_dfi_duplicate as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip
where ip.oprt_dto_dfi_duplicate = false

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'DFI' as hierarchy_level
, ip.dfi as account_number
, ip.dfi_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Account Owner' as ownership_level
, cast(ip.dfi_oprt_bed as varchar) as owner_bed
, ip.dfi_account_owner as owner_name
, ip.dfi_account_owner_type as owner_type
, ip.dfi_account_owner_region as owner_region
, ip.dfi_owner_segment_mapping as owner_segment_mapping
, ip.dfi_owner_responsiblity_mapping as owner_responsibility_mapping
, ip.dfi_account_owner_working_channel as owner_working_channel
, false as region_dto_duplicate
, false as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'KDNR' as hierarchy_level
, ip.cstm_kdnr as account_number
, ip.prtn_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Inside Sales' as ownership_level
, cast(ip.kdnr_bed_vkni as varchar) as owner_bed
, ip.kdnr_inside_sales as owner_name
, ip.kdnr_inside_sales_type as owner_type
, ip.kdnr_inside_sales_region as owner_region
, ip.kdnr_inside_segment_mapping as owner_segment_mapping
, ip.kdnr_inside_responsiblity_mapping as owner_responsibility_mapping
, ip.kdnr_inside_sales_working_channel as owner_working_channel
, ip.vkni_region_kdnr_dto_duplicate as region_dto_duplicate
, ip.vkni_region_kdnr_dfi_duplicate as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip
where ip.vkni_kdnr_dto_duplicate = false and ip.vkni_kdnr_dfi_duplicate = false

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'DTO' as hierarchy_level
, ip.dto as account_number
, ip.dto_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Inside Sales' as ownership_level
, cast(ip.dto_bed_vkni as varchar) as owner_bed
, ip.dto_inside_sales as owner_name
, ip.dto_inside_sales_type as owner_type
, ip.dto_inside_sales_region as owner_region
, ip.dto_inside_segment_mapping as owner_segment_mapping
, ip.dto_inside_responsiblity_mapping as owner_responsibility_mapping
, ip.dto_inside_sales_working_channel as owner_working_channel
, false as region_dto_duplicate
, ip.vkni_region_dto_dfi_duplicate as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip
where ip.vkni_dto_dfi_duplicate = false

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'DFI' as hierarchy_level
, ip.dfi as account_number
, ip.dfi_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Inside Sales' as ownership_level
, cast(ip.dfi_bed_vkni as varchar) as owner_bed
, ip.dfi_inside_sales as owner_name
, ip.dfi_inside_sales_type as owner_type
, ip.dfi_inside_sales_region as owner_region
, ip.dfi_inside_segment_mapping as owner_segment_mapping
, ip.dfi_inside_responsiblity_mapping as owner_responsibility_mapping
, ip.dfi_inside_sales_working_channel as owner_working_channel
, false as region_dto_duplicate
, false as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'KDNR' as hierarchy_level
, ip.cstm_kdnr as account_number
, ip.prtn_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Acquired By' as ownership_level
, cast(ip.kdnr_acquired_by_bed as varchar) as owner_bed
, ip.kdnr_acquired_by as owner_name
, ip.kdnr_acquired_by_type as owner_type
, ip.kdnr_acquired_by_region as owner_region
, ip.kdnr_acquired_segment_mapping as owner_segment_mapping
, ip.kdnr_acquired_responsiblity_mapping as owner_responsibility_mapping
, ip.kdnr_acquired_by_working_channel as owner_working_channel
, ip.acquired_region_kdnr_dto_duplicate as region_dto_duplicate
, ip.acquired_region_kdnr_dfi_duplicate as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip
where ip.acquired_by_kdnr_dto_duplicate = false and ip.acquired_by_kdnr_dfi_duplicate = false

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'DTO' as hierarchy_level
, ip.dto as account_number
, ip.dto_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Acquired By' as ownership_level
, cast(ip.dto_acquired_by_bed as varchar) as owner_bed
, ip.dto_acquired_by as owner_name
, ip.dto_acquired_by_type as owner_type
, ip.dto_acquired_by_region as owner_region
, ip.dto_acquired_segment_mapping as owner_segment_mapping
, ip.dto_acquired_responsiblity_mapping as owner_responsibility_mapping
, ip.dto_acquired_by_working_channel as owner_working_channel
, false as region_dto_duplicate
, ip.acquired_by_region_dto_dfi_duplicate as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip
where ip.acquired_by_dto_dfi_duplicate = false

union all 

select 
  ip.rntl_mvnr
, ip.rntl_mser
, ip.brnc_name
, ip.brnc_main_type
, ip.brnc_type
, ip.brnc_country_code_iso
, ip.brnc_country
, ip.brnc_corporate_franchise
, ip.brnc_country_region
, ip.brnc_country_region_franchise_breakdown

, 'DFI' as hierarchy_level
, ip.dfi as account_number
, ip.dfi_name as account_name
, ip.cstm_kdnr as kdnr
, ip.dto
, ip.dfi 
--, ip.cleansed_dfr --Learn what to do with this
, 'Acquired By' as ownership_level
, cast(ip.dfi_acquired_by_bed as varchar) as owner_bed
, ip.dfi_acquired_by as owner_name
, ip.dfi_acquired_by_type as owner_type
, ip.dfi_acquired_by_region as owner_region
, ip.dfi_acquired_segment_mapping as owner_segment_mapping
, ip.dfi_acquired_responsiblity_mapping as owner_responsibility_mapping
, ip.dfi_acquired_by_working_channel as owner_working_channel
, false as region_dto_duplicate
, false as region_dfi_duplicate

, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.app_reservation_resn
, ip.rsrv_cancelled_resn
, ip.rsrv_noshow_resn
, ip.internet_reservation_mvnr
, ip.app_reservation_mvnr
, ip.rntl_one_way_mvnr
, ip.rntl_correction_mvnr
, ip.domestic_country_mvnr
, ip.rntl_bev_mvnr
, ip.cstm_account_manager_num
, ip.cstm_account_manager_name
, ip.rntl_accounting_date
, ip.vhcl_group
, ip.vhcl_checked_out_group
, ip.vhcl_type
, ip.vhat_elty
, ip.AKTV

, ip.product_level1_source
, ip.product_level2_car_truck
, ip.product_level2_cnb_vnt
, ip.product_level3_name
, ip.product

, ip.rsts_excitement_num
, ip.rsts_recommendation_num
, ip.rntl_revenue
, ip.rntl_discount
, ip.rental_days
, ip.advanced_booking

, ip.internet_reservation_revenue
, ip.app_reservation_revenue
, ip.domestic_revenue
, ip.non_domestic_revenue
from initial_pull ip
)

select 
  uo.rntl_mvnr
, uo.rntl_mser
, uo.brnc_name
, uo.brnc_main_type
, uo.brnc_type
, uo.brnc_country_code_iso
, uo.brnc_country
, uo.brnc_corporate_franchise
, uo.brnc_country_region
, uo.brnc_country_region_franchise_breakdown
, uo.hierarchy_level
, uo.account_number
, uo.account_name
, uo.kdnr
, uo.dto
, uo.dfi 
, uo.ownership_level
, uo.owner_bed
, uo.owner_name
, uo.owner_type
, uo.owner_region
, uo.owner_segment_mapping
, uo.owner_responsibility_mapping
, uo.owner_working_channel
, uo.region_dto_duplicate
, uo.region_dfi_duplicate
, uo.kdnr_acquired_rent_date
, uo.sf_deal_amount
, uo.sf_total_sixt_potential
, uo.sf_total_customer_spend
, uo.app_reservation_resn
, uo.rsrv_cancelled_resn
, uo.rsrv_noshow_resn
, uo.internet_reservation_mvnr
, uo.app_reservation_mvnr
, uo.rntl_one_way_mvnr
, uo.rntl_correction_mvnr
, uo.domestic_country_mvnr
, uo.rntl_bev_mvnr
, uo.cstm_account_manager_num
, uo.cstm_account_manager_name
, uo.rntl_accounting_date
, uo.vhcl_group
, uo.vhcl_checked_out_group
, uo.vhcl_type
, uo.vhat_elty
, uo.AKTV
, uo.product_level1_source
, uo.product_level2_car_truck
, uo.product_level2_cnb_vnt
, uo.product_level3_name
, uo.product
, uo.rsts_excitement_num
, uo.rsts_recommendation_num
, uo.rntl_revenue
, uo.rntl_discount
, uo.rental_days
, uo.advanced_booking
, uo.internet_reservation_revenue
, uo.app_reservation_revenue
, uo.domestic_revenue
, uo.non_domestic_revenue
from unpivotted_ownership uo
where uo.owner_bed != '0' --To eliminate companies with no owner