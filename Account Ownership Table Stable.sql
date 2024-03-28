create table if not exists "sales_mart"."sales_account_ownership"
with (format='Parquet',
external_location='s3://sds-prod-store-marts/sales_mart/sales_account_ownership',
parquet_compression = 'SNAPPY') as
--Table Granularity is rs.rntl_mvnr, rs.rntl_mser, rs.rntl_konr, ch.chra_pos, ch.chra_inty + re.rsrv_resn
/*END: Specifying table destination*/


with cleansed_DFR as (
select pdp.prtn_kdnr
From "customer_shop"."pa_dim_partners" pdp
Left Join "customer_shop"."pa_dim_partners" pdo On (pdp.prtn_parent_num = pdo.prtn_kdnr)
Where pdo.prtn_registration_range_code <> 'DFR'
and pdp.prtn_registration_range_code  Not in ('DTT','DTO','DFI','DFR') 
and pdp.prtn_kdnr Not in (Select agnc_age 
                      From "customer_shop"."pa_dim_agencies"))


, cleansed_DFI as (
select pdp.agnc_age
From "customer_shop"."pa_dim_agencies" pdp
Left Join "customer_shop"."pa_dim_partners" pdo On (pdp.agnc_parent_num = pdo.prtn_kdnr)
Where pdo.prtn_registration_range_code <> 'DFI')


, agencies as (

/* There are 618 agnc_age that do not have a prtn_kdnr in "customer_shop"."pa_dim_partners"
TODO Add these 618 to the Account Hierarchy table 
select 
  count(*)
, sum(case when pa.prtn_kdnr is null then 1 else 0 end) 
from "customer_shop"."pa_dim_agencies" ag
left join  "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = ag.agnc_age

*/
select
  ag.agnc_age
, ag.agnc_name
, ag.oprt_bed

, 'AGE' as prtn_registration_range_code
, coalesce(concat('Agency: ',ag.agnc_type), 'Agency: Other') as agnc_type
, case when dfi.agnc_age is not null then False else True end as cleansed_dfi

, ag.agnc_subsidiary_new_num --TODO Compare to "customer_shop"."pa_dim_partners"
, ag.agnc_subsidiary_new_name
, ag.oprt_bed_subsidiary_new

, ag.agnc_parent_new_num --TODO Compare to "customer_shop"."pa_dim_partners"
, ag.agnc_parent_new_name
, ag.oprt_bed_parent_new

, ag.agnc_country_code
, ag.agnc_country
, ag.agnc_agency_specification
, ag.agnc_customer_classification
, ag.agnc_voucher_valid_from_datm
, ag.agnc_voucher_valid_to_datm

, ag.agnc_agency_blc_status_code
, ag.agnc_agency_blc_status_desc

, ag.agnc_person_region
, ag.agnc_working_channel

, ag.agnc_highest_person_region
, ag.agnc_highest_working_channel

from "customer_shop"."pa_dim_agencies" ag
left join cleansed_DFI dfi on dfi.agnc_age = ag.agnc_age
)


, partners as (

select 
  pa.prtn_kdnr 
, pa.prtn_kdnr_prospect
, pa.mndt_code
, pa.oprt_bed_vknr
, pa.oprt_bed_vkni
, pa.oprt_bed_kdbd
, pa.oprt_bed
, pa.oprt_bed_subsidiary
, pa.oprt_bed_subsidiary_calc
, pa.oprt_bed_parent
, pa.oprt_bed_parent_calc
, pa.oprt_bed_vkni_subsidiary
, pa.oprt_bed_vkni_subsidiary_calc
, pa.oprt_bed_vkni_parent
, pa.oprt_bed_vkni_parent_calc
, pa.prfl_pidn
, pa.prfl_role
, pa.prtn_customer_created_date
, pa.prtn_customer_created_dtid
, pa.prtn_blocked_status_code
, pa.prtn_blocked_status
, pa.prtn_registration_range_code
, pa.prtn_registration_range
, pa.prtn_name
, pa.prtn_name1
, pa.prtn_name2
, pa.prtn_name3
, pa.prtn_name1_paadr
, pa.prtn_subsidiary_num
, pa.prtn_subsidiary_name
, pa.prtn_subsidiary_name1
, pa.prtn_subsidiary_calc_num
, pa.prtn_subsidiary_calc_name
, pa.prtn_subsidiary_calc_name1
, pa.prtn_parent_num
, pa.prtn_parent_name
, pa.prtn_parent_name1
, pa.prtn_parent_calc_num
, pa.prtn_parent_calc_name
, pa.prtn_parent_calc_name1
, pa.prtn_house
, pa.prtn_po_box
, pa.prtn_street
, pa.prtn_postal_code
, pa.prtn_city
, pa.prtn_country
, pa.prtn_country_code
, pa.prtn_phone
, pa.prtn_fax
, pa.prtn_email
, pa.prtn_mobile_phone
, pa.prtn_highest_person_region
, pa.prtn_highest_working_channel
, pa.prtn_working_channel
, pa.prtn_cobra_sales_person_name
, pa.prtn_cobra_sales_manager_name
, pa.prtn_iscc_flg
, pa.prtn_report_email
, pa.prtn_report_flg
, pa.prtn_report_file_format
, pa.prtn_report_template_no
, pa.prtn_report_type
, pa.prtn_report_countries
, pa.prtn_classification_code
, pa.prtn_credit_limit_num
, pa.prtn_debit_card_code
, pa.prtn_locking_code_num
, pa.prtn_dunning_char
, pa.prtn_staff_acc_name_char
, pa.prtn_sxt_rating_code
, pa.prtn_pgs_rating_code
, pa.prtn_pgs_rating_desc
, pa.prtn_credit_rating_prio_code
, pa.prtn_credit_rating_prio_desc
, pa.prtn_formation_date
, pa.prtn_invoice_sending_code
, pa.prtn_invoice_sending_desc
, pa.prtn_invoice_collection_code
, pa.prtn_invoice_collection_desc
, pa.prtn_billing_period
, pa.prtn_pseudo_billing_flg
, pa.prtn_pay_condition_code
, pa.prtn_pay_condition_desc
, pa.prtn_ebilling_email
, pa.prtn_rate_priority_code
, pa.prtn_rate_priority_desc
, pa.prtn_potential_sales_num
, pa.prtn_collective_account_flg
, pa.prtn_regulator_num
, pa.prtn_leasing_region_code
, pa.prtn_analysis_code_1
, pa.prtn_analysis_code_2
, pa.prtn_analysis_code_3
, pa.prtn_analysis_code_4
, pa.prtn_customer_class_code
, pa.prtn_customer_class_desc
, pa.prtn_customer_changed_date
, pa.prtn_customer_changed_dtid
, pa.prtn_customer_changed_datm
, pa.prtn_customer_classification
, pa.prtn_account_id
, pa.prfl_industrial_sector_code
, pa.prfl_industrial_sector_desc
, pa.prtn_vat_id
, pa.prtn_persnr
, pa.prtn_tax_nr
, pa.prtn_sixt_account_nr
, pa.prtn_kdnr_registration_range_code
, pa.prtn_consolidated_company
, pa.prtn_dunning_key_code
, pa.prtn_dunning_key
, pa.prtn_unlimited_as
, pa.prtn_unlimited_kk
, pa.prtn_account_owner_type
, pa.prtn_acquired_by_rent_id
, pa.prtn_acquired_rent_date
, pa.prtn_acquired_by_rent_region
, rm2.mapping_segmentebene as acquired_segment_mapping
, rm2.mapping_betreuungsebene as acquired_responsiblity_mapping
, pa.prtn_person_region
, rm1.mapping_segmentebene as owner_segment_mapping
, rm1.mapping_betreuungsebene as owner_responsiblity_mapping
, pa.prtn_account_owner_person_region
, pa.prtn_acquired_by_rent_saleschannel
, pa.prtn_deal_amount
, pa.prtn_total_sixt_potential
, pa.prtn_total_customer_spend
, pa.prtn_inside_sales_person_region
, rm3.mapping_segmentebene as inside_segment_mapping
, rm3.mapping_betreuungsebene as inside_responsiblity_mapping
, pa.prtn_inside_sales_working_channel
, pa.sys_data_source
, pa.sys_taken_datm
, pa.sys_taken_grain_time
, pa.sys_user
, pa.sys_actual_flg
, pa.sys_deleted_flg
from "customer_shop"."pa_dim_partners" pa 
left join "sales_shop"."sales_controlling_region_mapping" rm1 on rm1.mapping_regionsebene = pa.prtn_person_region
left join "sales_shop"."sales_controlling_region_mapping" rm2 on rm2.mapping_regionsebene = pa.prtn_acquired_by_rent_region
left join "sales_shop"."sales_controlling_region_mapping" rm3 on rm3.mapping_regionsebene = pa.prtn_inside_sales_person_region
)

, enhanced_operator as (
select
  op.oprt_bed
, op.oprt_last_name
, op.oprt_first_name
, ucd.sucd_sales_person_type as oprt_sales_person_type
from "hr_shop"."op_dim_operators" op
left join "customer_shop"."sa_fct_user_config_data" ucd on ucd.sucd_personnel_number = cast(op.oprt_bed as varchar)
)


, revenue_by_country as (
select 
  pa.prtn_parent_calc_num
, br.brnc_country_code_iso
, sum(rs.rntl_revenue) as rntl_revenue 
from "rent_shop"."ra_fct_rental_series" rs
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = rs.cstm_kdnr
left join "common_shop"."br_dim_branches" br on br.brnc_code = rs.brnc_code_handover
where rs.rntl_accounting_date >= date_add('year', -1, current_date)
group by 1,2)


, ranked_revenue_by_country as (
select 
  prtn_parent_calc_num
, brnc_country_code_iso as domestic_country
, rntl_revenue 
, row_number() over (partition by prtn_parent_calc_num order by rntl_revenue desc) as country_revenue_rank 
from revenue_by_country)


, initial_pull as (
select 
  pa.prtn_kdnr
, pa.prtn_name
, pa.prtn_registration_range_code as abkz
, pa.prtn_blocked_status_code--Check AGAIN
, pa.prtn_blocked_status--Check AGAIN
, pa.prtn_rate_priority_code
, pa.prtn_rate_priority_desc
, dc.domestic_country as prtn_parent_domestic_country
, case when pa.prtn_parent_num = 0 and pa.prtn_subsidiary_num = 0 then 'Highest Account' else 'Linked Account' end as kdnr_highest_linked
, case when dfr.prtn_kdnr is not null then False else True end as cleansed_dfr

, pa.oprt_bed as kdnr_oprt_bed
, concat(op1.oprt_last_name,', ',op1.oprt_first_name) as kdnr_account_owner
, op1.oprt_sales_person_type as kdnr_account_owner_type
, pa.prtn_person_region as kdnr_account_owner_region --Team level
, pa.owner_segment_mapping as kdnr_owner_segment_mapping
, pa.owner_responsiblity_mapping as kdnr_owner_responsiblity_mapping
, pa.prtn_working_channel as kdnr_account_owner_working_channel --Sales Region

, pa.oprt_bed_vkni as kdnr_bed_vkni --Inside Sales
, concat(op4.oprt_last_name,', ',op4.oprt_first_name) as kdnr_inside_sales
, op4.oprt_sales_person_type as kdnr_inside_sales_type
, pa.prtn_inside_sales_person_region as kdnr_inside_sales_region
, pa.inside_segment_mapping as kdnr_inside_segment_mapping
, pa.inside_responsiblity_mapping as kdnr_inside_responsiblity_mapping
, pa.prtn_inside_sales_working_channel as kdnr_inside_sales_working_channel

, pa.prtn_acquired_by_rent_id as kdnr_acquired_by_bed
, concat(op7.oprt_last_name,', ',op7.oprt_first_name) as kdnr_acquired_by
, op7.oprt_sales_person_type as kdnr_acquired_by_type
, pa.prtn_acquired_by_rent_region as kdnr_acquired_by_region
, pa.acquired_segment_mapping as kdnr_acquired_segment_mapping
, pa.acquired_responsiblity_mapping as kdnr_acquired_responsiblity_mapping
, pa.prtn_acquired_by_rent_saleschannel as kdnr_acquired_by_working_channel
, pa.prtn_acquired_rent_date as kdnr_acquired_rent_date
, pa.prtn_deal_amount as sf_deal_amount
, pa.prtn_total_sixt_potential as sf_total_sixt_potential
, pa.prtn_total_customer_spend as sf_total_customer_spend


, pa.prtn_subsidiary_calc_num as dto
, pa.prtn_subsidiary_calc_name as dto_name

, dto.oprt_bed as dto_oprt_bed
, concat(op2.oprt_last_name,', ',op2.oprt_first_name) as dto_account_owner
, op2.oprt_sales_person_type as dto_account_owner_type
, dto.prtn_person_region as dto_account_owner_region --Team level
, dto.owner_segment_mapping as dto_owner_segment_mapping
, dto.owner_responsiblity_mapping as dto_owner_responsiblity_mapping
, dto.prtn_working_channel as dto_account_owner_working_channel --Sales Region

, dto.oprt_bed_vkni as dto_bed_vkni --Inside Sales
, concat(op5.oprt_last_name,', ',op5.oprt_first_name) as dto_inside_sales
, op5.oprt_sales_person_type as dto_inside_sales_type
, dto.prtn_inside_sales_person_region as dto_inside_sales_region
, dto.inside_segment_mapping as dto_inside_segment_mapping
, dto.inside_responsiblity_mapping as dto_inside_responsiblity_mapping
, dto.prtn_inside_sales_working_channel as dto_inside_sales_working_channel

, dto.prtn_acquired_by_rent_id as dto_acquired_by_bed
, concat(op8.oprt_last_name,', ',op8.oprt_first_name) as dto_acquired_by
, op8.oprt_sales_person_type as dto_acquired_by_type
, dto.prtn_acquired_by_rent_region as dto_acquired_by_region
, dto.acquired_segment_mapping as dto_acquired_segment_mapping
, dto.acquired_responsiblity_mapping as dto_acquired_responsiblity_mapping
, dto.prtn_acquired_by_rent_saleschannel as dto_acquired_by_working_channel


, pa.prtn_parent_calc_num as dfi
, pa.prtn_parent_calc_name as dfi_name

, dfi.oprt_bed as dfi_oprt_bed
, concat(op3.oprt_last_name,', ',op3.oprt_first_name) as dfi_account_owner
, op3.oprt_sales_person_type as dfi_account_owner_type
, dfi.prtn_person_region as dfi_account_owner_region --Team level
, dfi.owner_segment_mapping as dfi_owner_segment_mapping
, dfi.owner_responsiblity_mapping as dfi_owner_responsiblity_mapping
, dfi.prtn_working_channel as dfi_account_owner_working_channel --Sales Region

, dfi.oprt_bed_vkni as dfi_bed_vkni --Inside Sales
, concat(op6.oprt_last_name,', ',op6.oprt_first_name) as dfi_inside_sales
, op6.oprt_sales_person_type as dfi_inside_sales_type
, dfi.prtn_inside_sales_person_region as dfi_inside_sales_region
, dfi.inside_segment_mapping as dfi_inside_segment_mapping
, dfi.inside_responsiblity_mapping as dfi_inside_responsiblity_mapping
, dfi.prtn_inside_sales_working_channel as dfi_inside_sales_working_channel

, dfi.prtn_acquired_by_rent_id as dfi_acquired_by_bed
, concat(op9.oprt_last_name,', ',op9.oprt_first_name) as dfi_acquired_by
, op9.oprt_sales_person_type as dfi_acquired_by_type
, dfi.prtn_acquired_by_rent_region as dfi_acquired_by_region
, dfi.acquired_segment_mapping as dfi_acquired_segment_mapping
, dfi.acquired_responsiblity_mapping as dfi_acquired_responsiblity_mapping
, dfi.prtn_acquired_by_rent_saleschannel as dfi_acquired_by_working_channel

from partners pa 
left join ranked_revenue_by_country dc on dc.prtn_parent_calc_num = pa.prtn_parent_calc_num and dc.country_revenue_rank = 1
left join partners dto on dto.prtn_kdnr = pa.prtn_subsidiary_calc_num
left join partners dfi on dfi.prtn_kdnr = pa.prtn_parent_calc_num
left join enhanced_operator op1 on op1.oprt_bed = pa.oprt_bed
left join enhanced_operator op2 on op2.oprt_bed = dto.oprt_bed
left join enhanced_operator op3 on op3.oprt_bed = dfi.oprt_bed
left join enhanced_operator op4 on op4.oprt_bed = pa.oprt_bed_vkni
left join enhanced_operator op5 on op5.oprt_bed = dto.oprt_bed_vkni
left join enhanced_operator op6 on op6.oprt_bed = dfi.oprt_bed_vkni
left join enhanced_operator op7 on cast(op7.oprt_bed as varchar) = pa.prtn_acquired_by_rent_id
left join enhanced_operator op8 on cast(op8.oprt_bed as varchar) = dto.prtn_acquired_by_rent_id
left join enhanced_operator op9 on cast(op9.oprt_bed as varchar) = dfi.prtn_acquired_by_rent_id
left join cleansed_DFR dfr on dfr.prtn_kdnr = pa.prtn_kdnr)

, intermediary_pull as (
select
  ip.prtn_kdnr
, ip.prtn_name
, ip.abkz
, ip.prtn_blocked_status_code
, ip.prtn_blocked_status
, ip.prtn_rate_priority_code
, ip.prtn_rate_priority_desc
, ip.prtn_parent_domestic_country
, ip.kdnr_highest_linked
, ip.cleansed_dfr

, ip.kdnr_oprt_bed
, ip.kdnr_account_owner
, ip.kdnr_account_owner_type
, ip.kdnr_account_owner_region
, ip.kdnr_owner_segment_mapping
, ip.kdnr_owner_responsiblity_mapping
, ip.kdnr_account_owner_working_channel
, case when ip.kdnr_oprt_bed = ip.dto_oprt_bed and ip.kdnr_oprt_bed != 0 and ip.kdnr_oprt_bed is not null then True else False end as oprt_kdnr_dto_duplicate
, case when ip.kdnr_oprt_bed = ip.dfi_oprt_bed and ip.kdnr_oprt_bed != 0 and ip.kdnr_oprt_bed is not null then True else False end as oprt_kdnr_dfi_duplicate
, case when ip.kdnr_account_owner_region = ip.dto_account_owner_region and ip.kdnr_account_owner_region != '' and ip.kdnr_account_owner_region is not null then True else False end as oprt_region_kdnr_dto_duplicate
, case when ip.kdnr_account_owner_region = ip.dfi_account_owner_region and ip.kdnr_account_owner_region != '' and ip.kdnr_account_owner_region is not null then True else False end as oprt_region_kdnr_dfi_duplicate

, ip.kdnr_bed_vkni
, ip.kdnr_inside_sales
, ip.kdnr_inside_sales_type
, ip.kdnr_inside_sales_region
, ip.kdnr_inside_segment_mapping
, ip.kdnr_inside_responsiblity_mapping
, ip.kdnr_inside_sales_working_channel
, case when ip.kdnr_bed_vkni = ip.dto_bed_vkni and ip.kdnr_bed_vkni != 0 and ip.kdnr_bed_vkni is not null then True else False end as  vkni_kdnr_dto_duplicate
, case when ip.kdnr_bed_vkni = ip.dfi_bed_vkni and ip.kdnr_bed_vkni != 0 and ip.kdnr_bed_vkni is not null then True else False end as  vkni_kdnr_dfi_duplicate
, case when ip.kdnr_inside_sales_region = ip.dto_inside_sales_region and ip.kdnr_inside_sales_region != '' and ip.kdnr_inside_sales_region is not null then True else False end as vkni_region_kdnr_dto_duplicate
, case when ip.kdnr_inside_sales_region = ip.dfi_inside_sales_region and ip.kdnr_inside_sales_region != '' and ip.kdnr_inside_sales_region is not null then True else False end as vkni_region_kdnr_dfi_duplicate

, ip.kdnr_acquired_by_bed
, ip.kdnr_acquired_by
, ip.kdnr_acquired_by_type
, ip.kdnr_acquired_by_region
, ip.kdnr_acquired_segment_mapping
, ip.kdnr_acquired_responsiblity_mapping
, ip.kdnr_acquired_by_working_channel
, case when ip.kdnr_acquired_by_bed = ip.dto_acquired_by_bed and ip.kdnr_acquired_by_bed != '0' and ip.kdnr_acquired_by_bed is not null then True else False end as  acquired_by_kdnr_dto_duplicate
, case when ip.kdnr_acquired_by_bed = ip.dfi_acquired_by_bed and ip.kdnr_acquired_by_bed != '0' and ip.kdnr_acquired_by_bed is not null then True else False end as  acquired_by_kdnr_dfi_duplicate
, case when ip.kdnr_acquired_by_region = ip.dto_acquired_by_region and ip.kdnr_acquired_by_region != '' and ip.kdnr_acquired_by_region is not null then True else False end as acquired_region_kdnr_dto_duplicate
, case when ip.kdnr_acquired_by_region = ip.dfi_acquired_by_region and ip.kdnr_acquired_by_region != '' and ip.kdnr_acquired_by_region is not null then True else False end as acquired_region_kdnr_dfi_duplicate
, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend


, ip.dto
, ip.dto_name

, ip.dto_oprt_bed
, ip.dto_account_owner
, ip.dto_account_owner_type
, ip.dto_account_owner_region
, ip.dto_owner_segment_mapping
, ip.dto_owner_responsiblity_mapping
, ip.dto_account_owner_working_channel
, case when ip.dto_oprt_bed = ip.dfi_oprt_bed and ip.dto_oprt_bed != 0 and ip.dto_oprt_bed is not null then True else False end as oprt_dto_dfi_duplicate
, case when ip.dto_account_owner_region = ip.dfi_account_owner_region and ip.dto_account_owner_region != '' and ip.dto_account_owner_region is not null then True else False end as oprt_region_dto_dfi_duplicate

, ip.dto_bed_vkni
, ip.dto_inside_sales
, ip.dto_inside_sales_type
, ip.dto_inside_sales_region
, ip.dto_inside_segment_mapping
, ip.dto_inside_responsiblity_mapping
, ip.dto_inside_sales_working_channel
, case when ip.dto_bed_vkni = ip.dfi_bed_vkni and ip.dto_bed_vkni != 0 and ip.dto_bed_vkni is not null then True else False end as vkni_dto_dfi_duplicate
, case when ip.dto_inside_sales_region = ip.dfi_inside_sales_region and ip.dto_inside_sales_region != '' and ip.dto_inside_sales_region is not null then True else False end as vkni_region_dto_dfi_duplicate

, ip.dto_acquired_by_bed
, ip.dto_acquired_by
, ip.dto_acquired_by_type
, ip.dto_acquired_by_region
, ip.dto_acquired_segment_mapping
, ip.dto_acquired_responsiblity_mapping
, ip.dto_acquired_by_working_channel
, case when ip.dto_acquired_by_bed = ip.dfi_acquired_by_bed and ip.dto_acquired_by_bed != '0' and ip.dto_acquired_by_bed is not null then True else False end as acquired_by_dto_dfi_duplicate
, case when ip.dto_acquired_by_region = ip.dfi_acquired_by_region and ip.dto_acquired_by_region != '' and ip.dto_acquired_by_region is not null then True else False end as acquired_by_region_dto_dfi_duplicate



, ip.dfi
, ip.dfi_name

, ip.dfi_oprt_bed
, ip.dfi_account_owner
, ip.dfi_account_owner_type
, ip.dfi_account_owner_region
, ip.dfi_owner_segment_mapping
, ip.dfi_owner_responsiblity_mapping
, ip.dfi_account_owner_working_channel

, ip.dfi_bed_vkni
, ip.dfi_inside_sales
, ip.dfi_inside_sales_type
, ip.dfi_inside_sales_region
, ip.dfi_inside_segment_mapping
, ip.dfi_inside_responsiblity_mapping
, ip.dfi_inside_sales_working_channel

, ip.dfi_acquired_by_bed
, ip.dfi_acquired_by
, ip.dfi_acquired_by_type
, ip.dfi_acquired_by_region
, ip.dfi_acquired_segment_mapping
, ip.dfi_acquired_responsiblity_mapping
, ip.dfi_acquired_by_working_channel
from initial_pull ip
)

select
  ip.prtn_kdnr
, ip.prtn_name
, ip.abkz
, ip.prtn_blocked_status_code
, ip.prtn_blocked_status
, ip.prtn_rate_priority_code
, ip.prtn_rate_priority_desc
, ip.prtn_parent_domestic_country
, ip.kdnr_highest_linked
, ip.cleansed_dfr

, ip.kdnr_oprt_bed
, ip.kdnr_account_owner
, ip.kdnr_account_owner_type
, ip.kdnr_account_owner_region
, ip.kdnr_owner_segment_mapping
, ip.kdnr_owner_responsiblity_mapping
, ip.kdnr_account_owner_working_channel
, ip.oprt_kdnr_dto_duplicate
, ip.oprt_kdnr_dfi_duplicate
, ip.oprt_region_kdnr_dto_duplicate
, ip.oprt_region_kdnr_dfi_duplicate

, ip.kdnr_bed_vkni
, ip.kdnr_inside_sales
, ip.kdnr_inside_sales_type
, ip.kdnr_inside_sales_region
, ip.kdnr_inside_segment_mapping
, ip.kdnr_inside_responsiblity_mapping
, ip.kdnr_inside_sales_working_channel
, ip.vkni_kdnr_dto_duplicate
, ip.vkni_kdnr_dfi_duplicate
, ip.vkni_region_kdnr_dto_duplicate
, ip.vkni_region_kdnr_dfi_duplicate

, ip.kdnr_acquired_by_bed
, ip.kdnr_acquired_by
, ip.kdnr_acquired_by_type
, ip.kdnr_acquired_by_region
, ip.kdnr_acquired_segment_mapping
, ip.kdnr_acquired_responsiblity_mapping
, ip.kdnr_acquired_by_working_channel
, ip.acquired_by_kdnr_dto_duplicate
, ip.acquired_by_kdnr_dfi_duplicate
, ip.acquired_region_kdnr_dto_duplicate
, ip.acquired_region_kdnr_dfi_duplicate
, ip.kdnr_acquired_rent_date
, ip.sf_deal_amount
, ip.sf_total_sixt_potential
, ip.sf_total_customer_spend

, ip.dto
, ip.dto_name
, ip.dto_oprt_bed
, ip.dto_account_owner
, ip.dto_account_owner_type
, ip.dto_account_owner_region
, ip.dto_owner_segment_mapping
, ip.dto_owner_responsiblity_mapping
, ip.dto_account_owner_working_channel
, ip.oprt_dto_dfi_duplicate
, ip.oprt_region_dto_dfi_duplicate
, ip.dto_bed_vkni
, ip.dto_inside_sales
, ip.dto_inside_sales_type
, ip.dto_inside_sales_region
, ip.dto_inside_segment_mapping
, ip.dto_inside_responsiblity_mapping
, ip.dto_inside_sales_working_channel
, ip.vkni_dto_dfi_duplicate
, ip.vkni_region_dto_dfi_duplicate
, ip.dto_acquired_by_bed
, ip.dto_acquired_by
, ip.dto_acquired_by_type
, ip.dto_acquired_by_region
, ip.dto_acquired_segment_mapping
, ip.dto_acquired_responsiblity_mapping
, ip.dto_acquired_by_working_channel
, ip.acquired_by_dto_dfi_duplicate
, ip.acquired_by_region_dto_dfi_duplicate
, ip.dfi
, ip.dfi_name
, ip.dfi_oprt_bed
, ip.dfi_account_owner
, ip.dfi_account_owner_type
, ip.dfi_account_owner_region
, ip.dfi_owner_segment_mapping
, ip.dfi_owner_responsiblity_mapping
, ip.dfi_account_owner_working_channel
, ip.dfi_bed_vkni
, ip.dfi_inside_sales
, ip.dfi_inside_sales_type
, ip.dfi_inside_sales_region
, ip.dfi_inside_segment_mapping
, ip.dfi_inside_responsiblity_mapping
, ip.dfi_inside_sales_working_channel
, ip.dfi_acquired_by_bed
, ip.dfi_acquired_by
, ip.dfi_acquired_by_type
, ip.dfi_acquired_by_region
, ip.dfi_acquired_segment_mapping
, ip.dfi_acquired_responsiblity_mapping
, ip.dfi_acquired_by_working_channel
from intermediary_pull ip