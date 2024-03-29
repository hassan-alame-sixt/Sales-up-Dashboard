/*START: Specifying table destination*/
create table if not exists "sales_mart"."self_service_main_table"
with (format='Parquet',
external_location='s3://sds-prod-store-marts/sales_mart/self_service_main_table',
parquet_compression = 'SNAPPY') as
--Table Granularity is rs.rntl_mvnr, rs.rntl_mser, rs.rntl_konr, ch.chra_pos, ch.chra_inty + re.rsrv_resn
/*END: Specifying table destination*/

--Two filters on Reservations fact table 

With sixt_unlimited as (
select 
  r.prtn_kdnr
, r.rnsb_aonr
, r.mndt_code 
, r.rnsb_status
, r.vhcl_group
, r.sys_actual_flg
, Cast(r.rnsb_handover_date as date) AS start_date
, Cast(r.rnsb_return_date as date) AS end_date
, Cast(g.date_date as date) as calendar_date
, (((r.rnsb_month_rate) / Extract(Day From (date_add('month', 1, date_trunc('month', g.date_date)) - date_trunc('month', g.date_date))))/ (gd.exrd_exchange_rate)) as Revenue_per_day
From "rent_shop"."ra_fct_subscriptions" r
Left Join "common_shop"."ge_dim_dates" g on (Cast(g.date_date as date) between Cast(rnsb_handover_date as date) and Cast(rnsb_return_date as date))
left Join "common_shop"."ge_dim_daily_exchange_rates" gd On (Replace(r.rnsb_currency_code,'CHF','SFR') = gd.exrd_foreign_currency_code)
Where exrd_rate_date = date_trunc('month', Cast(g.date_date as date))
)


, inc_charges as ( --Maintenance complication
select
ch.rntl_mvnr,
ch.rntl_mser,
ch.rntl_konr,
sum(case when cinc_origin_code in ('M','R') then cinc_total_amount else 0 end) as Inc_tot
-- count(distinct case when cinc_origin_code in ('M','R') and cinc_total_amount > 0 then chrg_chco end) as Inc_tot_CC,
-- SUM(CASE WHEN cinc_origin_code = 'M' and Oprt_Bed_Handover <> 9000038505 THEN cinc_total_amount END) AS cinc_total_amount_M,
-- count (distinct case when  cinc_origin_code = 'M' and Oprt_Bed_Handover <> 9000038505 and cinc_total_amount > 0 THEN chrg_chco END)  AS cinc_total_cc_M,
-- SUM(CASE WHEN (cinc_origin_code = 'R' AND cinc_chl2 in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null)) THEN cinc_total_amount END) AS cinc_total_amount_R_digital,
-- count(distinct case when (cinc_origin_code = 'R' AND cinc_chl2 in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null)) and cinc_total_amount > 0 THEN chrg_chco END)  AS cinc_total_cc_R_Digital,
-- SUM(CASE WHEN cinc_chl3 = 'DIGITAL_CHECKOUT' or (cinc_origin_code ='M' and Oprt_Bed_Handover = 9000038505) THEN cinc_total_amount END) AS cinc_total_amount_R_Xpress,
-- count (distinct CASE WHEN cinc_chl3 = 'DIGITAL_CHECKOUT' or (cinc_origin_code ='M' and Oprt_Bed_Handover = 9000038505) and cinc_total_amount > 0 THEN chrg_chco END)  AS cinc_total_cc_R_Xpress,
-- SUM(CASE WHEN cinc_origin_code = 'R' and (cinc_chl2 not in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') or cinc_chl2 is null) and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null) THEN cinc_total_amount END) AS cinc_total_amount_R_other,
-- count (distinct case when cinc_origin_code = 'R' and (cinc_chl2 not in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') or cinc_chl2 is null) and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null) and cinc_total_amount > 0 THEN chrg_chco END) AS cinc_total_cc_R_other
from sds_prod_rent_gg_dwh_current.ch_fct_incremental_charges ch
join (select rntl_mvnr, rntl_mser, max(rntl_konr) as rntl_konr
       from sds_prod_rent_gg_dwh_current.ch_fct_incremental_charges
       group by 1,2) cc on ch.rntl_mvnr = cc.rntl_mvnr and ch.rntl_mser = cc.rntl_mser and ch.rntl_konr = cc.rntl_konr
join (select fir, chco 
       from sales_migration.ravparm 
       where vdat > date('2023-07-01') and chco not in ('S','X','T','K') group by 1,2) ic on ic.fir = ch.mndt_code and ic.chco = ch.chrg_chco
where rate_prl in (select rate_prl from sds_prod_rent_gg_dwh_current.rt_dim_rates where rate_incr_rev_relevant_flg = 1)
-- and ch.rntl_mvnr =9497670359
-- and ch.sys_deleted_flg = 0 and ch.sys_actual_flg = 1 
group by 1,2,3)

, initial_pull as (
select
  re.rsrv_resn
, re.rsrv_new_customer -- New vs. Existing ???
, re.rsrv_status
, re.rsrv_status_extended
, re.rsrv_source_chl1
, re.rsrv_source_chl2
, re.rsrv_source_chl3
, re.rsrv_yield_source -- channels 
, re.rsrv_yield_source_level2
, re.rsrv_yield_source_level3  
--, re.mndt_code
--, re.mndt_code_pos
, re.rsrv_posl_country_code

, rs.rntl_mvnr  
, rs.rntl_mser
, rs.rntl_konr

, rs.brnc_code_handover
, br.brnc_name
, br.brnc_operator
, br.brnc_main_type
, br.brnc_type_code
, br.brnc_type
, br.brnc_pool_code
, br.brnc_pool_name
, br.brnc_continent
, br.brnc_country_code_iso
, br.brnc_country
, brnc_active_flg 


, case when br.brnc_country_code_iso in ('DE','AT','CH','FR','MC','GB','IT','ES','BE','NL','LU','US','CA') then 'Corporate' 
       else 'Franchise' 
       end as brnc_corporate_franchise
, case when br.brnc_country_code_iso = 'DE' then 'Germany'
   when br.brnc_country_code_iso = 'AT' then 'Austria'
   when br.brnc_country_code_iso = 'CH' then 'Switzerland'
   when br.brnc_country_code_iso in ('FR','MC') then 'France'
   when br.brnc_country_code_iso = 'GB' then 'Great Britain'
   when br.brnc_country_code_iso = 'IT' then 'Italy'
   when br.brnc_country_code_iso = 'ES' then 'Spain'
   when br.brnc_country_code_iso in ('BE','NL','LU') then 'BeNeLux'
   when br.brnc_country_code_iso = 'US' then 'United States'
   when br.brnc_country_code_iso = 'CA' then 'Canada'
   else 'Franchise' 
   end as brnc_country_region
, case when br.brnc_country_code_iso = 'DE' then 'Germany'
    when br.brnc_country_code_iso = 'AT' then 'Austria'
    when br.brnc_country_code_iso = 'CH' then 'Switzerland'
    when br.brnc_country_code_iso in ('FR','MC') then 'France'
    when br.brnc_country_code_iso = 'GB' then 'Great Britain'
    when br.brnc_country_code_iso = 'IT' then 'Italy'
    when br.brnc_country_code_iso = 'ES' then 'Spain'
    when br.brnc_country_code_iso in ('BE','NL','LU') then 'BeNeLux'
    when br.brnc_country_code_iso = 'US' then 'United States'
    when br.brnc_country_code_iso = 'CA' then 'Canada'
    when br.brnc_country_code_iso in ('AR','BZ','BO','BV','BR','CL','CO'
                                ,'CR','EC','SV','FK','GF','GT','GY'
                                ,'HN','MX','NI','PA','PY','PE','GS'
                                ,'SR','UY','VE') then 'LatAm'
    when br.brnc_country_code_iso in ('BH','CY','IR','IQ','JO','KW','LB'
                                ,'OM','QA','SA','SY','AE','YE') then 'MidEast'
    when br.brnc_country_code_iso in ('DK','FI','NO','SE') then 'Scandanavia'
    else 'Other Franchise' 
    end as brnc_country_region_franchise_breakdown    
, br.brnc_region
--, br.brnc_state_code
, br.brnc_state
, br.brnc_city

, case when dfr.prtn_kdnr is null then False else True end as cleansed_dfr
, case when dfi.agnc_age is null then False else True end as cleansed_dfi

, rs.cstm_kdnr
, pa.prtn_registration_range_code as abkz
, pa.prtn_name as cstm_name
, pa.oprt_bed as kdnr_prt_bed --Account Owner
, pa.oprt_bed_vkni as kdnr_bed_vkni --Inside Sales
, pa.prtn_blocked_status_code--Check AGAIN
, pa.prtn_blocked_status--Check AGAIN
, pa.prtn_highest_person_region as kdnr_highest_person_region
, pa.prtn_highest_working_channel as kdnr_highest_working_channel
, pa.prtn_person_region as kdnr_prtn_person_region --Team level
, pa.prtn_working_channel as kdnr_prtn_working_channel --Sales Region
, concat(op1.oprt_first_name, ' ', op1.oprt_last_name) as kdnr_account_owner
, case when pa.prtn_parent_num = 0 and pa.prtn_subsidiary_num = 0 then 'Highest Account' else 'Linked Account' end as kdnr_highest_linked

, pa.prtn_subsidiary_calc_num as dto
, pa.prtn_subsidiary_calc_name as dto_name

, pa.prtn_parent_calc_num as dfi
, pa.prtn_parent_calc_name as dfi_name
, dc.domestic_country as prtn_parent_domestic_country

, pa.prtn_account_owner_type as sf_account_owner_type
, pa.prtn_acquired_by_rent_id as sf_acquired_by_rent_id
, pa.prtn_acquired_rent_date as sf_acquired_rent_date
, pa.prtn_acquired_by_rent_region as sf_acquired_by_rent_region
, pa.prtn_inside_sales_person_region as sf_inside_sales_person_region
, pa.prtn_acquired_by_rent_saleschannel as sf_acquired_by_rent_saleschannel
, pa.prtn_deal_amount as sf_deal_amount
, pa.prtn_total_sixt_potential as sf_total_sixt_potential
, pa.prtn_total_customer_spend as sf_total_customer_spend

, rs.agnc_age_agency1 as agnc_age
, ag.prtn_name as age_name
, ag.oprt_bed as age_prt_bed
, ag.prtn_highest_person_region as age_highest_person_region --Team level
, ag.prtn_highest_working_channel as age_highest_working_channel --Sales Region
, ag.prtn_person_region as age_prtn_person_region
, ag.prtn_working_channel as age_prtn_working_channel
, concat(op2.oprt_last_name,', ',op2.oprt_first_name) as age_account_owner
, case when ag.prtn_parent_num = 0 and ag.prtn_subsidiary_num = 0 then 'Highest Account' else 'Linked Account' end as age_highest_linked

, ag.prtn_subsidiary_calc_num as dtt
, ag.prtn_subsidiary_calc_name as dtt_name

, ag.prtn_parent_calc_num as dfr
, ag.prtn_parent_calc_name as dfr_name

, rs.rntl_type_code
, rs.rntl_type
, rs.rntl_payment_type

, case when re.rsrv_yield_source_level2 in ('Internet Public','Internet Else','Internet Corporate') then re.rsrv_resn 
       else null 
       end as internet_reservation_resn
, case when re.rsrv_yield_source_level2 = 'Mobile' then re.rsrv_resn 
       else null 
       end as app_reservation_resn
, case when re.rsrv_cancelled_flg = 1 then re.rsrv_resn 
       else null 
       end as rsrv_cancelled_resn
, case when re.rsrv_noshow_flg = 1 then re.rsrv_resn 
       else null 
       end as rsrv_noshow_resn 

, case when re.rsrv_yield_source_level2 in ('Internet Public','Internet Else','Internet Corporate') then rs.rntl_mvnr 
       else null 
       end as internet_reservation_mvnr
, case when re.rsrv_yield_source_level2 = 'Mobile' then rs.rntl_mvnr 
       else null 
       end as app_reservation_mvnr
, case when rs.brnc_code_handover = rs.brnc_code_return then rs.rntl_mvnr 
       else null 
       end as rntl_one_way_mvnr
, case when max(rs.rntl_konr) over (partition by rs.rntl_mvnr) = 0 then rs.rntl_mvnr 
       else null 
       end as rntl_correction_mvnr 
, case when br.brnc_country = dc.domestic_country then rs.rntl_mvnr
       else null 
       end as domestic_country_mvnr
, case when min(ve.vhat_elty) over (partition by rs.rntl_mvnr) = 'E' then rs.rntl_mvnr 
       else null 
       end as rntl_bev_mvnr


, rs.cstm_account_manager_num
, rs.cstm_account_manager_name

, re.rsrv_date
, rs.rntl_handover_datm
, rs.rntl_return_datm
, rs.rntl_accounting_date


, rs.vhgr_crs -- Paid Vehicle Group: connected to price list
, rs.vhcl_group
, rs.vhcl_checked_out_group
--, ve.vhcl_group
, ve.vhcl_type
, ve.vhcl_category_level1
, ve.vhcl_category_level2
, ve.vhcl_category_level3
, ve.vhcl_category_level4
, ve.vhcl_owner_status
, ve.vhat_elty
    
    
, rs.rate_id
, rs.rate_prl -- Rate Code
, rt.rate_bundle
, rt.rate_type
, rt.rate_designation

, rs.rate_type_level1_gare
, rs.rate_type_level2_glev --Tariff
, rs.rate_type_level3_aknm
, rs.rate_type_level4_aktv --3 and 4 are connected include both
, concat(rs.rate_type_level4_aktv,' - ', rs.rate_type_level3_aknm) as AKTV
, rt.rate_crm_type_gare_clv

, rt.rate_gdat
, rt.rate_vdat
, rt.rate_next_gdat
, coalesce(date_add('day',-1,rt.rate_next_gdat),rt.rate_vdat) as rate_validity_end


, 'Rent' as product_level1_source
-- Product Level 2 (Car or Truck)
, case  when rt.vhcl_yield_type_code = 'P'              then 'Car'
        when rt.vhcl_yield_type_code = 'L'              then 'Truck'
        else rt.vhcl_yield_type_code
        end as product_level2_car_truck
-- Product Level 2 (C&B or V&T) - same logic, different wording. Used internally. Kept for compatibility reasons.
, case  when rt.vhcl_yield_type_code = 'P'              then 'C&B' -- Cars & Busses
        when rt.vhcl_yield_type_code = 'L'              then 'V&T' -- Vans & Trucks
        else rt.vhcl_yield_type_code
        end as product_level2_cnb_vnt
-- Product Level 3 (Product Name)
, case  when rs.rate_type_level1_gare = 'Subscription'  then rs.rate_type_level3_aknm
        when rs.rntl_type_code = 'Long'                 then 'Long-Term'
        when rs.rntl_type_code = 'Short'                then 'Short-Term'
        else rs.rntl_type_code
        end as product_level3_name

--  ch.chra_mvnr
--, ch.chra_mser
--, ch.chra_konr
, ch.chra_pos
, ch.chra_inty
, ch.chra_chco
, cn.chrg_name 

--MVNR Granularity case when rs.rntl_mser = 0 and (ch.chra_mvnr is null or (ch.chra_pos = 1 and ch.chra_inty = 'M')) 
, case when rs.rntl_mser = 0 and (ch.chra_mvnr is null or (ch.chra_pos = 1 and ch.chra_inty = 'M')) then cs.rsts_excitement_num 
       else null 
       end as rsts_excitement_num--To attach only 1 rntl_revenue per mvnr 
, case when rs.rntl_mser = 0 and (ch.chra_mvnr is null or (ch.chra_pos = 1 and ch.chra_inty = 'M')) then cs.rsts_recommendation_num 
       else null 
       end as rsts_recommendation_num--To attach only 1 rntl_revenue per mvnr 

--MSER Granularity
, case when ch.chra_mvnr is null then rs.rntl_revenue 
       when ch.chra_mvnr is not null and ch.chra_pos = 1 and ch.chra_inty = 'M' then rs.rntl_revenue 
       else null 
       end as rntl_revenue--To attach only 1 rntl_revenue per mser
, case when ch.chra_mvnr is null then rs.rntl_discount
       when ch.chra_mvnr is not null and ch.chra_pos = 1 and ch.chra_inty = 'M' then rs.rntl_discount 
       else null 
       end as rntl_discount--To attach only 1 rntl_revenue per mser
, case when ch.chra_mvnr is null then rs.rntl_rental_days
       when ch.chra_mvnr is not null and ch.chra_pos = 1 and ch.chra_inty = 'M' then rs.rntl_rental_days 
       else null 
       end as rental_days--To attach only 1 rntl_revenue per mser
, case when ch.chra_mvnr is null then date_diff('day', re.rsrv_date, rs.rntl_handover_date)
       when ch.chra_mvnr is not null and ch.chra_pos = 1 and ch.chra_inty = 'M' then date_diff('day', re.rsrv_date, rs.rntl_handover_date)
       else null 
       end as advanced_booking--To attach only 1 rntl_revenue per mser

--CHCO Granularity
, ch.chra_value
, case when cn.chrg_level1_care = 'Time & Mileage' then ch.chra_value 
       else null 
       end as Time_and_Mileage
, case when ch.chra_mvnr is null then rs.rntl_revenue
       when ((count(*) over (partition by ch.chra_mvnr, ch.chra_mser, ch.chra_konr)- count(ch.chra_value) over (partition by ch.chra_mvnr, ch.chra_mser, ch.chra_konr)) != 0) and ch.chra_value is null then 1.0*(rs.rntl_revenue- coalesce(sum(ch.chra_value) over (partition by ch.chra_mvnr, ch.chra_mser, ch.chra_konr),0) +rs.rntl_discount)/(count(*) over (partition by ch.chra_mvnr, ch.chra_mser, ch.chra_konr)-count(ch.chra_value) over (partition by ch.chra_mvnr, ch.chra_mser, ch.chra_konr))
       else ch.chra_value - 1.0*rs.rntl_discount/(count(*) over (partition by ch.chra_mvnr, ch.chra_mser, ch.chra_konr)) 
       end as improved_revenue
, case when ch.chra_mvnr is null then inc.Inc_tot
       when ch.chra_mvnr is not null and ch.chra_pos = 1 and ch.chra_inty = 'M' then inc.Inc_tot
       else null 
       end as Inc_tot--To attach only 1 rntl_revenue per mvnr


from "rent_shop"."ra_fct_rental_series" rs
full join "rent_shop"."rs_fct_reservations" re on re.rntl_mvnr = rs.rntl_mvnr
left join "customer_shop"."ra_fct_satisfactions" cs on cs.rntl_mvnr = rs.rntl_mvnr
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = rs.cstm_kdnr
left join "customer_shop"."pa_dim_partners" ag on ag.prtn_kdnr = rs.agnc_age_agency1
left join ranked_revenue_by_country dc on dc.prtn_parent_calc_num = pa.prtn_parent_calc_num and dc.country_revenue_rank = 1
left join "hr_shop"."op_dim_operators" op1 on op1.oprt_bed = pa.oprt_bed 
left join "hr_shop"."op_dim_operators" op2 on op2.oprt_bed = ag.oprt_bed
left join user_config_data on ucd on ucd.sucd_personnel_number = pa.oprt_bed
left join "common_shop"."br_dim_branches" br on br.brnc_code = rs.brnc_code_handover
left join "fleet_shop"."ve_dim_vehicles" ve on ve.vhcl_int_num = rs.vhcl_int_num
left join cleansed_DFR dfr on dfr.prtn_kdnr = rs.cstm_kdnr
left join cleansed_DFI dfi on dfi.agnc_age = rs.agnc_age_agency1
left join "rent_shop"."rt_dim_rates" rt
    on (rs.rate_prl = rt.rate_prl and rs.rntl_handover_date between rt.rate_gdat and coalesce(date_add('day',-1,rt.rate_next_gdat),rt.rate_vdat)) 
left join inc_charges inc on inc.rntl_mvnr = rs.rntl_mvnr
    and inc.rntl_mser = rs.rntl_mser
    and inc.rntl_konr = rs.rntl_konr
left join "rent_shop"."ch_fct_ra_charges" ch -- charge usage/revenue
    on ch.chra_mvnr = rs.rntl_mvnr
    and ch.chra_mser = rs.rntl_mser
    and ch.chra_konr = rs.rntl_konr
    -- sum over chrg_inty ! -> Main + Secondary invoice
left join "rent_shop"."ch_dim_charges" cn         -- charge code names
    on cn.chrg_chco = ch.chra_chco
    where year (rs.rntl_accounting_date)  >= 2019) 


select 
  i.rsrv_resn
, i.rsrv_new_customer
, i.rsrv_status
, i.rsrv_status_extended
, i.rsrv_source_chl1
, i.rsrv_source_chl2
, i.rsrv_source_chl3
, i.rsrv_yield_source
, i.rsrv_yield_source_level2
, i.rsrv_yield_source_level3
, i.rsrv_posl_country_code
, i.rntl_mvnr
, i.rntl_mser
, i.rntl_konr
, i.brnc_code_handover
, i.brnc_name
, i.brnc_operator
, i.brnc_main_type
, i.brnc_type_code
, i.brnc_type
, i.brnc_pool_code
, i.brnc_pool_name
, i.brnc_continent
, i.brnc_country_code_iso
, i.brnc_country
, i.brnc_active_flg
, i.brnc_corporate_franchise
, i.brnc_country_region
, i.brnc_country_region_franchise_breakdown
, i.brnc_region
, i.brnc_state
, i.brnc_city
, i.cleansed_dfr
, i.cleansed_dfi
, i.cstm_kdnr
, i.abkz
, i.cstm_name
, i.kdnr_prt_bed
, i.kdnr_bed_vkni
, case when ((i.dfi != 6507  or i.dfi is null) AND i.brnc_type_code = 'A') then 'GAM - Germany' else i.kdnr_highest_person_region end as kdnr_highest_person_region --AAFES: Set highest level region
, i.kdnr_highest_working_channel
, i.kdnr_prtn_person_region
, case when ((i.dfi != 6507  or i.dfi is null) AND i.brnc_type_code = 'A') then 'Global Accounts' else i.kdnr_prtn_working_channel end as kdnr_prtn_working_channel --AAFES: Set highest level working channel
, case when ((i.dfi != 6507  or i.dfi is null) AND i.brnc_type_code = 'A') then 'Gulden' else i.kdnr_account_owner end as kdnr_account_owner --AAFES: Set default account owner 
, i.kdnr_highest_linked
, i.dto
, i.dto_name
, case when ((i.dfi != 6507  or i.dfi is null) AND i.brnc_type_code = 'A') then 19434019 else i.dfi end as dfi --AAFES: Set default account number
, case when ((i.dfi != 6507  or i.dfi is null) AND i.brnc_type_code = 'A') then 'AAFES Dummy account'  else  i.dfi_name end as dfi_name --AAFES: Set default account name 
, i.prtn_parent_domestic_country

, i.sf_account_owner_type
, i.sf_acquired_by_rent_id
, i.sf_acquired_rent_date
, i.sf_acquired_by_rent_region
, i.sf_inside_sales_person_region
, i.sf_acquired_by_rent_saleschannel
, i.sf_deal_amount
, i.sf_total_sixt_potential
, i.sf_total_customer_spend

, i.agnc_age
, i.age_name
, i.age_prt_bed
, i.age_highest_person_region
, i.age_highest_working_channel
, i.age_prtn_person_region
, i.age_prtn_working_channel
, i.age_account_owner
, i.age_highest_linked
, i.dtt
, i.dtt_name
, i.dfr
, i.dfr_name
, i.rntl_type_code
, i.rntl_type
, i.rntl_payment_type
, i.internet_reservation_resn
, i.app_reservation_resn
, i.rsrv_cancelled_resn
, i.rsrv_noshow_resn
, i.internet_reservation_mvnr
, i.app_reservation_mvnr
, i.rntl_one_way_mvnr
, i.rntl_correction_mvnr
, i.domestic_country_mvnr
, i.rntl_bev_mvnr
, i.cstm_account_manager_num
, i.cstm_account_manager_name
, i.rsrv_date
, i.rntl_handover_datm
, i.rntl_return_datm
, i.rntl_accounting_date
, i.vhgr_crs
, i.vhcl_group
, i.vhcl_checked_out_group
, i.vhcl_type
, i.vhcl_category_level1
, i.vhcl_category_level2
, i.vhcl_category_level3
, i.vhcl_category_level4
, i.vhcl_owner_status
, i.vhat_elty
, i.rate_id
, i.rate_prl
, i.rate_bundle
, i.rate_type
, i.rate_designation
, i.rate_type_level1_gare
, i.rate_type_level2_glev
, i.rate_type_level3_aknm
, i.rate_type_level4_aktv
, i.AKTV
, i.rate_crm_type_gare_clv
, i.rate_gdat
, i.rate_vdat
, i.rate_next_gdat
, i.rate_validity_end
, i.product_level1_source
, i.product_level2_car_truck
, i.product_level2_cnb_vnt
, i.product_level3_name
, concatenate(i.product_level2_car_truck, ' - ', i.product_level3_name) as product
, i.chra_pos
, i.chra_inty
, i.chra_chco
, i.chrg_name
, i.rsts_excitement_num
, i.rsts_recommendation_num
, i.rntl_revenue
, i.rntl_discount
, i.rental_days
, i.advanced_booking
, i.chra_value
, i.Time_and_Mileage
, i.improved_revenue
, i.Inc_tot

, case when i.internet_reservation_mvnr is not null then i.improved_revenue
       else null 
       end as internet_reservation_revenue
, case when i.app_reservation_mvnr is not null then i.improved_revenue
       else null 
       end as app_reservation_revenue
, case when i.domestic_country_mvnr is not null then i.improved_revenue
       else null 
       end as domestic_revenue
, case when i.domestic_country_mvnr is null then i.improved_revenue
       else null 
       end as non_domestic_revenue

from initial_pull i
where year(i.rntl_accounting_date) >= 2019

/*
--Rental Card Status
, rs.rntl_card_status_level1
, rs.rntl_card_status_level2
, rs.rntl_card_status_level3
, rs.rntl_card_status_level3_name 
/*

--Flags
, rs.rntl_unlimited_flg
, rs.rntl_delivery_flg
, rs.rntl_collection_flg
, rs.rntl_fastlane_flg
, rs.rntl_smartstart_flg
, rs.rntl_self_service_flg
, rs.rntl_salesboost_flg
, rs.rntl_cobra_checkout_flg
, rs.rntl_waiting_flg
, re.rntl_changed_vehicles_num
*/

/*
-- Rental Other Currencies
, rs.rntl_revenue_rental--Original If controlling asks for local currency they usually mean this 
, rs.rntl_rental_currency_code
, rs.rntl_revenue_local_currency --Pickup country's local currency
, rs.rntl_local_currency_code
*/

/*
--Other Currencies
, ch.rntl_rental_currency_code as rntl_rental_currency_code_2
, ch.rntl_local_currency_code as rntl_local_currency_code_2
, ch.rntl_paid_currency_code as rntl_paid_currency_code_2
, ch.rntl_exchange_rate
, ch.rntl_exchange_rate_rental
, ch.chra_unit_value
, ch.chra_unit_value_rental
, ch.chra_unit_value_local
, ch.chra_unit_value_paid
, ch.chra_unit_nu
, ch.chra_value_rental
, ch.chra_value_local
*/ 

/*
--Rate Details
  rt.rate_prl
, rt.rate_gdat
, rt.rate_vdat
, rt.rate_next_gdat
, coalesce(date_add('day',-1,rt.rate_next_gdat),rt.rate_vdat)) as rate_validity_end
, rt.rate_bundle
, rt.rate_type
, rt.rate_business_area_product
, rt.rate_type_level1_gare
, rt.rate_type_level2_glev
, rt.rate_type_level3_aknm
, rt.rate_type_level4_aktv
, rt.rate_crm_type_gare_clv
, rt.rate_designation
left join "rent_shop"."rt_dim_rates" rt on rt.rate_prl = rs.rate_prl
*/


/*
--Old SalesForce Pull
, sales_force as (
select 
  "meta.timestamp"
, "company_data.customer_number" as prtn_kdnr
, "inside_sales.account_owner_type" as account_owner_type --2
, "inside_sales.acquired_by_rent_id" as acquired_by_rent_id --3
, "inside_sales.acquired_rent_date" as acquired_rent_date --4
, "inside_sales.acquired_by_rent_region" as acquired_by_rent_region --5
, "inside_sales.account_owner_region" as inside_sales_person_region --10
, "inside_sales.acquired_by_rent_saleschannel" as acquired_by_rent_saleschannel --7
, "potential_information.deal_amount" as deal_amount --6
, "potential_information.total_sixt_potential_without_row" as total_sixt_potential --8
, "potential_information.total_customer_spend_without_row" as total_customer_spend --9
, row_number() over (partition by "company_data.customer_number" order by "meta.timestamp" desc) as rn
from "sds_prod_ingestion_store_public_datalake"."customerorgv1" c)
*/