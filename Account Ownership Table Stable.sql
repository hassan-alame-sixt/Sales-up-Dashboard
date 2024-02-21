create table if not exists "sales_mart"."sales_account_ownership"
with (format='Parquet',
external_location='s3://sds-prod-store-marts/sales_mart/sales_account_ownership',
parquet_compression = 'SNAPPY') as
--Table Granularity is rs.rntl_mvnr, rs.rntl_mser, rs.rntl_konr, ch.chra_pos, ch.chra_inty + re.rsrv_resn
/*END: Specifying table destination*/

With enchanced_operator as 
(
select
  op.oprt_bed
, op.mndt_code
, op.brnc_code
, op.oprt_active_flg
, op.oprt_region_code
, op.oprt_region
, op.oprt_last_name
, op.oprt_first_name
, op.oprt_title_code
, op.oprt_title
, op.oprt_department
, op.oprt_group_code
, op.oprt_group
, op.oprt_bed_char
, op.oprt_login_name
, op.oprt_email
, ucd.sucd_sales_person_tpye as oprt_sales_person_type
, op.sys_data_source
, op.sys_taken_datm
, op.sys_taken_grain_time
, op.sys_user
, op.sys_actual_flg
, op.sys_deleted_flg
from "hr_shop"."op_dim_operators" op
left join "customer_shop"."sa_fct_user_config_data" ucd on ucd.sucd_personnel_number = op.oprt_bed
)


, initial_pull as (
select 
  pa.prtn_kdnr
, pa.prtn_name

, pa.oprt_bed as kdnr_oprt_bed
, concat(op1.oprt_last_name,', ',op1.oprt_first_name) as kdnr_account_owner
, op1.oprt_sales_person_type kdnr_account_owner_type
, pa.prtn_person_region as kdnr_account_owner_person_region --Team level
, pa.prtn_working_channel as kdnr_account_owner_working_channel --Sales Region

, pa.oprt_bed_vkni as kdnr_bed_vkni --Inside Sales
, concat(op4.oprt_last_name,', ',op4.oprt_first_name) as kdnr_inside_sales
, op4.oprt_sales_person_type kdnr_inside_sales_type
, pa.prtn_inside_sales_person_region as kdnr_inside_sales_person_region
, pa.prtn_inside_sales_working_channel as kdnr_inside_sales_working_channel

, pa.prtn_acquired_by_rent_id as kdnr_acquired_by_rent_id
, concat(op7.oprt_last_name,', ',op7.oprt_first_name) as kdnr_acquired_by
, op7.oprt_sales_person_type kdnr_acquired_by_type
, pa.prtn_acquired_by_rent_region as kdnr_acquired_by_rent_region
, pa.prtn_acquired_by_rent_saleschannel as kdnr_acquired_by_rent_saleschannel


, pa.prtn_subsidiary_calc_num as dto
, pa.prtn_subsidiary_calc_name as dto_name

, dto.oprt_bed as dto_oprt_bed
, concat(op2.oprt_last_name,', ',op2.oprt_first_name) as dto_account_owner
, op2.oprt_sales_person_type dto_account_owner_type
, dto.prtn_person_region as dto_account_owner_person_region --Team level
, dto.prtn_working_channel as dto_account_owner_working_channel --Sales Region

, dto.oprt_bed_vkni as dto_bed_vkni --Inside Sales
, concat(op5.oprt_last_name,', ',op5.oprt_first_name) as dto_inside_sales
, op5.oprt_sales_person_type as dto_inside_sales_type
, dto.prtn_inside_sales_person_region as dto_inside_sales_person_region
, dto.prtn_inside_sales_working_channel as dto_inside_sales_working_channel

, dto.prtn_acquired_by_rent_id as dto_acquired_by_rent_id
, concat(op8.oprt_last_name,', ',op8.oprt_first_name) as dto_acquired_by
, op8.oprt_sales_person_type dto_acquired_by_type
, dto.prtn_acquired_by_rent_region as dto_acquired_by_rent_region
, dto.prtn_acquired_by_rent_saleschannel as dto_acquired_by_rent_saleschannel


, pa.prtn_parent_calc_num as dfi
, pa.prtn_parent_calc_name as dfi_name

, dfi.oprt_bed as dfi_oprt_bed
, concat(op3.oprt_last_name,', ',op3.oprt_first_name) as dfi_account_owner
, op3.oprt_sales_person_type dfi_account_owner_type
, dfi.prtn_person_region as dfi_account_owner_person_region --Team level
, dfi.prtn_working_channel as dfi_account_owner_working_channel --Sales Region

, dfi.oprt_bed_vkni as dfi_bed_vkni --Inside Sales
, concat(op6.oprt_last_name,', ',op6.oprt_first_name) as dfi_inside_sales
, op6.oprt_sales_person_type dfi_inside_sales_type
, dfi.prtn_inside_sales_person_region as dfi_inside_sales_person_region
, dfi.prtn_inside_sales_working_channel as dfi_inside_sales_working_channel

, dfi.prtn_acquired_by_rent_id as dfi_acquired_by_rent_id
, concat(op9.oprt_last_name,', ',op9.oprt_first_name) as dfi_acquired_by
, op9.oprt_sales_person_type dfi_acquired_by_type
, dfi.prtn_acquired_by_rent_region as dfi_acquired_by_rent_region
, dfi.prtn_acquired_by_rent_saleschannel as dfi_acquired_by_rent_saleschannel

from "customer_shop"."pa_dim_partners" pa 
left join "customer_shop"."pa_dim_partners" dto on dto.prtn_kdnr = pa.prtn_subsidiary_calc_num
left join "customer_shop"."pa_dim_partners" dfi on dfi.prtn_kdnr = pa.prtn_parent_calc_num
left join enchanced_operator op1 on op1.oprt_bed = pa.oprt_bed
left join enchanced_operator op2 on op2.oprt_bed = dto.oprt_bed
left join enchanced_operator op3 on op3.oprt_bed = dfi.oprt_bed
left join enchanced_operator op4 on op4.oprt_bed = pa.oprt_bed_vkni
left join enchanced_operator op5 on op5.oprt_bed = dto.oprt_bed_vkni
left join enchanced_operator op6 on op6.oprt_bed = dfi.oprt_bed_vkni
left join enchanced_operator op7 on cast(op7.oprt_bed as varchar) = pa.prtn_acquired_by_rent_id
left join enchanced_operator op8 on cast(op8.oprt_bed as varchar) = dto.prtn_acquired_by_rent_id
left join enchanced_operator op9 on cast(op9.oprt_bed as varchar) = dfi.prtn_acquired_by_rent_id)

, intermediary_pull as (
select
  ip.prtn_kdnr
, ip.prtn_name
, ip.kdnr_oprt_bed
, ip.kdnr_account_owner
, ip.kdnr_account_owner_type
, ip.kdnr_account_owner_person_region
, ip.kdnr_account_owner_working_channel
, case when ip.kdnr_oprt_bed = ip.dto_oprt_bed and ip.kdnr_oprt_bed != 0 and ip.kdnr_oprt_bed is not null and ip.prtn_kdnr != ip.dto then True else False end as oprt_kdnr_dto_duplicate
, case when ip.kdnr_oprt_bed = ip.dfi_oprt_bed and ip.kdnr_oprt_bed != 0 and ip.kdnr_oprt_bed is not null and ip.prtn_kdnr != ip.dfi then True else False end as  oprt_kdnr_dfi_duplicate

, ip.kdnr_bed_vkni
, ip.kdnr_inside_sales
, ip.kdnr_inside_sales_type
, ip.kdnr_inside_sales_person_region
, ip.kdnr_inside_sales_working_channel
, case when ip.kdnr_bed_vkni = ip.dto_bed_vkni and ip.kdnr_bed_vkni != 0 and ip.kdnr_bed_vkni is not null and ip.prtn_kdnr != ip.dto then True else False end as  vkni_kdnr_dto_duplicate
, case when ip.kdnr_bed_vkni = ip.dfi_bed_vkni and ip.kdnr_bed_vkni != 0 and ip.kdnr_bed_vkni is not null and ip.prtn_kdnr != ip.dfi then True else False end as  vkni_kdnr_dfi_duplicate

, ip.kdnr_acquired_by_rent_id
, ip.kdnr_acquired_by
, ip.kdnr_acquired_by_type
, ip.kdnr_acquired_by_rent_region
, ip.kdnr_acquired_by_rent_saleschannel
, case when ip.kdnr_acquired_by_rent_id = ip.dto_acquired_by_rent_id and ip.kdnr_acquired_by_rent_id != '0' and ip.kdnr_acquired_by_rent_id is not null and ip.prtn_kdnr != ip.dto then True else False end as  acquired_by_kdnr_dto_duplicate
, case when ip.kdnr_acquired_by_rent_id = ip.dfi_acquired_by_rent_id and ip.kdnr_acquired_by_rent_id != '0' and ip.kdnr_acquired_by_rent_id is not null and ip.prtn_kdnr != ip.dfi then True else False end as  acquired_by_kdnr_dfi_duplicate


, ip.dto
, ip.dto_name
, ip.dto_oprt_bed
, ip.dto_account_owner
, ip.dto_account_owner_type
, ip.dto_account_owner_person_region
, ip.dto_account_owner_working_channel
, case when ip.dto_oprt_bed = ip.dfi_oprt_bed and ip.dto_oprt_bed != 0 and ip.dto_oprt_bed is not null and ip.dto != ip.dfi then True else False end as oprt_dto_dfi_duplicate

, ip.dto_bed_vkni
, ip.dto_inside_sales
, ip.dto_inside_sales_type
, ip.dto_inside_sales_person_region
, ip.dto_inside_sales_working_channel
, case when ip.dto_bed_vkni = ip.dfi_bed_vkni and ip.dto_bed_vkni != 0 and ip.dto_bed_vkni is not null  and ip.dto != ip.dfi  then True else False end as vkni_dto_dfi_duplicate

, ip.dto_acquired_by_rent_id
, ip.dto_acquired_by
, ip.dto_acquired_by_type
, ip.dto_acquired_by_rent_region
, ip.dto_acquired_by_rent_saleschannel
, case when ip.dto_acquired_by_rent_id = ip.dfi_acquired_by_rent_id and ip.dto_acquired_by_rent_id != '0' and ip.dto_acquired_by_rent_id is not null and ip.dto != ip.dfi  then True else False end as acquired_by_dto_dfi_duplicate

, ip.dfi
, ip.dfi_name
, ip.dfi_oprt_bed
, ip.dfi_account_owner
, ip.dfi_account_owner_type
, ip.dfi_account_owner_person_region
, ip.dfi_account_owner_working_channel
, ip.dfi_bed_vkni
, ip.dfi_inside_sales
, ip.dfi_inside_sales_type
, ip.dfi_inside_sales_person_region
, ip.dfi_inside_sales_working_channel
, ip.dfi_acquired_by_rent_id
, ip.dfi_acquired_by
, ip.dfi_acquired_by_type
, ip.dfi_acquired_by_rent_region
, ip.dfi_acquired_by_rent_saleschannel
from initial_pull ip
)

select
  ip.prtn_kdnr
, ip.prtn_name
, ip.kdnr_oprt_bed
, ip.kdnr_account_owner
, ip.kdnr_account_owner_type
, ip.kdnr_account_owner_person_region
, ip.kdnr_account_owner_working_channel
, ip.oprt_kdnr_dto_duplicate
, ip.oprt_kdnr_dfi_duplicate

, ip.kdnr_bed_vkni
, ip.kdnr_inside_sales
, ip.kdnr_inside_sales_type
, ip.kdnr_inside_sales_person_region
, ip.kdnr_inside_sales_working_channel
, ip.vkni_kdnr_dto_duplicate
, ip.vkni_kdnr_dfi_duplicate

, ip.kdnr_acquired_by_rent_id
, ip.kdnr_acquired_by
, ip.kdnr_acquired_by_type
, ip.kdnr_acquired_by_rent_region
, ip.kdnr_acquired_by_rent_saleschannel
, ip.acquired_by_kdnr_dto_duplicate
, ip.acquired_by_kdnr_dfi_duplicate


, ip.dto
, ip.dto_name
, ip.dto_oprt_bed
, ip.dto_account_owner
, ip.dto_account_owner_type
, ip.dto_account_owner_person_region
, ip.dto_account_owner_working_channel
, ip.oprt_dto_dfi_duplicate

, ip.dto_bed_vkni
, ip.dto_inside_sales
, ip.dto_inside_sales_type
, ip.dto_inside_sales_person_region
, ip.dto_inside_sales_working_channel
, ip.vkni_dto_dfi_duplicate

, ip.dto_acquired_by_rent_id
, ip.dto_acquired_by
, ip.dto_acquired_by_type
, ip.dto_acquired_by_rent_region
, ip.dto_acquired_by_rent_saleschannel
, ip.acquired_by_dto_dfi_duplicate

, ip.dfi
, ip.dfi_name
, ip.dfi_oprt_bed
, ip.dfi_account_owner
, ip.dfi_account_owner_type
, ip.dfi_account_owner_person_region
, ip.dfi_account_owner_working_channel
, ip.dfi_bed_vkni
, ip.dfi_inside_sales
, ip.dfi_inside_sales_type
, ip.dfi_inside_sales_person_region
, ip.dfi_inside_sales_working_channel
, ip.dfi_acquired_by_rent_id
, ip.dfi_acquired_by
, ip.dfi_acquired_by_type
, ip.dfi_acquired_by_rent_region
, ip.dfi_acquired_by_rent_saleschannel
from intermediary_pull ip

