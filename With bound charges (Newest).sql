create table if not exists "sales_mart"."pos_analysis_bound_revenue"
with (format='Parquet',

external_location='s3://sds-prod-store-marts/sales_mart/pos_analysis_bound_revenue',

parquet_compression = 'SNAPPY') as

with inc_charges as (
    select
    ch.rntl_mvnr,
    ch.rntl_mser,
    ch.rntl_konr,
    sum(case when cinc_origin_code in ('M','R') then cinc_total_amount else 0 end) as Inc_tot
--    count(distinct case when cinc_origin_code in ('M','R') and cinc_total_amount > 0 then chrg_chco end) as Inc_tot_CC,
--    SUM(CASE WHEN cinc_origin_code = 'M' and Oprt_Bed_Handover <> 9000038505 THEN cinc_total_amount END) AS cinc_total_amount_M,
--    count (distinct case when  cinc_origin_code = 'M' and Oprt_Bed_Handover <> 9000038505 and cinc_total_amount > 0 THEN chrg_chco END)  AS cinc_total_cc_M,
--    SUM(CASE WHEN (cinc_origin_code = 'R' AND cinc_chl2 in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null)) THEN cinc_total_amount END) AS cinc_total_amount_R_digital,
--    count(distinct case when (cinc_origin_code = 'R' AND cinc_chl2 in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null)) and cinc_total_amount > 0 THEN chrg_chco END)  AS cinc_total_cc_R_Digital,
--    SUM(CASE WHEN cinc_chl3 = 'DIGITAL_CHECKOUT' or (cinc_origin_code ='M' and Oprt_Bed_Handover = 9000038505) THEN cinc_total_amount END) AS cinc_total_amount_R_Xpress,
--    count (distinct CASE WHEN cinc_chl3 = 'DIGITAL_CHECKOUT' or (cinc_origin_code ='M' and Oprt_Bed_Handover = 9000038505) and cinc_total_amount > 0 THEN chrg_chco END)  AS cinc_total_cc_R_Xpress,
--    SUM(CASE WHEN cinc_origin_code = 'R' and (cinc_chl2 not in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') or cinc_chl2 is null) and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null) THEN cinc_total_amount END) AS cinc_total_amount_R_other,
--    count (distinct case when cinc_origin_code = 'R' and (cinc_chl2 not in ('iSixt/Android','iSixt/iPhone','iSixt/iPad','Website Public','Website Agent','Website Corporate') or cinc_chl2 is null) and (cinc_chl3 <> 'DIGITAL_CHECKOUT' or cinc_chl3 is null) and cinc_total_amount > 0 THEN chrg_chco END) AS cinc_total_cc_R_other
    from sds_prod_rent_gg_dwh_current.ch_fct_incremental_charges ch
    join (select rntl_mvnr, rntl_mser, max(rntl_konr) as rntl_konr
          from sds_prod_rent_gg_dwh_current.ch_fct_incremental_charges
          group by 1,2) cc on ch.rntl_mvnr = cc.rntl_mvnr and ch.rntl_mser = cc.rntl_mser and ch.rntl_konr = cc.rntl_konr
    join (select fir, chco 
          from sales_migration.ravparm 
          where vdat > date('2023-07-01') and chco not in ('S','X','T','K') group by 1,2) ic on ic.fir = ch.mndt_code and ic.chco = ch.chrg_chco
    where rate_prl in (select rate_prl from sds_prod_rent_gg_dwh_current.rt_dim_rates where rate_incr_rev_relevant_flg = 1)
--    and ch.rntl_mvnr =9497670359
--    and ch.sys_deleted_flg = 0 and ch.sys_actual_flg = 1 
    group by 1,2,3
    )

, charges as (
select
    ch.chra_mvnr
    , ch.chra_mser
    , ch.chra_konr
    , sum(ch.chra_value) as total_charges
    , sum(case when chrg_level1_care = 'Time & Mileage' then ch.chra_value else null end) as Time_and_Mileage
--    , sum(case when chrg_level1_care = 'Surcharges' then ch.chra_value else null end) as Surcharges
--    , sum(case when chrg_level2_clev = 'Extras' then ch.chra_value else null end) as Extras
    from "rent_shop"."ch_fct_ra_charges" ch
    left join "rent_shop"."ch_dim_charges" cn         -- charge code names
        on cn.chrg_chco = ch.chra_chco
    group by 1,2,3
    )

, initial_pull as (
select
    a.rntl_mvnr
    , a.rntl_mser
    , a.rntl_konr
    , row_number() over (partition by  a.rntl_mvnr, a.rntl_mser, a.rntl_konr order by b.date_date) as row_count
    , country_mser_handover as brnc_country_code_iso_handover 
    , a.ndat_begin
    , a.ndat_end
    , b.date_date
    , rsrv_source_chl1 as rsrv_source_chl1
    , scd_lv0 as rntl_scd_lv0
    , case when scd_lv0 = 'B2P' then agency1_parent_name else scd_lv2 end as rntl_scd_drillthrough
    , br.brnc_main_type as brnc_main_type
    , pool_mser_handover as brnc_pool_code
    , poolname_mser_handover as brnc_pool_name
    , region_mser_handover as brnc_region_code
    , regionname_mser_handover as brnc_region
    , brnc_code_mser_handover as brnc_code
    , brncname_mser_handover as brnc_name
    , rntl_driver_origin as prst_address_country_iso_code
    , vhgr_category_level2_ra as vhgr_category_level2_booked
    , case when rntl_driver_origin = 'DE' then 'Germany'
        when rntl_driver_origin = 'AT' then 'Austria'
        when rntl_driver_origin = 'CH' then 'Switzerland'
        when rntl_driver_origin in ('FR','MC') then 'France'
        when rntl_driver_origin = 'GB' then 'Great Britain'
        when rntl_driver_origin = 'IT' then 'Italy'
        when rntl_driver_origin = 'ES' then 'Spain'
        when rntl_driver_origin in ('BE','NL','LU') then 'BeNeLux'
        when rntl_driver_origin = 'US' then 'United States'
        when rntl_driver_origin = 'CA' then 'Canada'
        when rntl_driver_origin in ('AR','BZ','BO','BV','BR','CL','CO'
                                    ,'CR','EC','SV','FK','GF','GT','GY'
                                    ,'HN','MX','NI','PA','PY','PE','GS'
                                    ,'SR','UY','VE') then 'LatAm'
        when rntl_driver_origin in ('BH','CY','IR','IQ','JO','KW','LB'
                                    ,'OM','QA','SA','SY','AE','YE') then 'MidEast'
        when rntl_driver_origin in ('DK','FI','NO','SE') then 'Scandanavia'
        else 'Other Franchise' 
        end as country_region    
    , a.ndat_corporate_revenue_loc
    , a.rntl_exchange_rate
    , 1.0*a.ndat_corporate_revenue_loc/a.rntl_exchange_rate as ndat_corporate_revenue_euro
    , sum(1.0*a.ndat_corporate_revenue_loc/a.rntl_exchange_rate) over (partition by  a.rntl_mvnr, a.rntl_mser, a.rntl_konr) as total_ndat_corporate_revenue_euro
    from "yield_shop"."ca_yield_ra_fct_bound_yield" a
    left join "common_shop"."br_dim_branches" br on br.brnc_code = a.brnc_code_mser_handover
    , "common_shop"."ge_dim_dates" b
    where format_datetime(b.date_date,'YYYY-MM-dd') between format_datetime(a.ndat_begin,'YYYY-MM-dd') and format_datetime(a.ndat_end,'YYYY-MM-dd')
        and vhgr_category_level1_ra = 'C&B' 
        and rntl_type_code = 'Short'
        and country_mser_handover in ('DE','AT','CH','FR','MC','GB','IT','ES','BE','NL','LU','US','CA') 
--        and rntl_driver_origin in ('DE','AT','CH','FR','MC','GB','IT','ES','BE','NL','LU','US','CA') 
        and year(b.date_date) in (2023,2022,2019)
--        and a.rntl_mvnr = 9497670359
    order by date_date
    )


select
  a.rntl_mvnr
, a.rntl_mser
, a.rntl_konr
, a.row_count
, a.brnc_country_code_iso_handover
, a.ndat_begin
, a.ndat_end
, a.date_date
, a.rsrv_source_chl1
, a.rntl_scd_lv0
, a.rntl_scd_drillthrough
, a.brnc_main_type
, a.brnc_pool_code
, a.brnc_pool_name
, a.brnc_region_code
, a.brnc_region
, a.brnc_code
, a.brnc_name
, a.prst_address_country_iso_code
, a.vhgr_category_level2_booked
, a.country_region
, a.ndat_corporate_revenue_loc
, a.rntl_exchange_rate
, a.ndat_corporate_revenue_euro
, case when a.row_count = 1 then a.total_ndat_corporate_revenue_euro else 0 end as total_ndat_corporate_revenue_euro
, case when a.row_count = 1 then ch.total_charges else 0 end as total_charges
, case when a.row_count = 1 then ch.Time_and_Mileage else 0 end as Time_and_Mileage
, case when a.row_count = 1 then inc.Inc_tot else 0 end as Inc_tot
, case when a.row_count = 1 then ch.total_charges - ch.Time_and_Mileage - inc.Inc_tot else 0 end as other_charges
, coalesce(1.0*ch.total_charges * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro,0) as bound_total_charges
, coalesce(1.0*ch.Time_and_Mileage * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro,0) as bound_Time_and_Mileage
, coalesce(1.0*inc.Inc_tot * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro,0) as bound_Inc_total
, coalesce(1.0*(ch.total_charges - ch.Time_and_Mileage - inc.Inc_tot) * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro,0) as bound_other_chages
--, case when a.row_count = 1 then ch.Surcharges else null end as Surcharges
--, case when a.row_count = 1 then ch.Extras else null end as Extras
--, 1.0*ch.Surcharges * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro as bound_Surcharges
--, 1.0*ch.Extras * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro as bound__Extras
from initial_pull a
left join charges ch on ch.chra_mvnr = a.rntl_mvnr
    and ch.chra_mser = a.rntl_mser
    and ch.chra_konr = a.rntl_konr
left join inc_charges inc on inc.rntl_mvnr = a.rntl_mvnr
    and inc.rntl_mser = a.rntl_mser
    and inc.rntl_konr = a.rntl_konr
