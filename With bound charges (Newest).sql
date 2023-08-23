with charges as (
select
  ch.chra_mvnr
, ch.chra_mser
, ch.chra_konr
, sum(ch.chra_value) as total_charges
, sum(case when chrg_level1_care = 'Time & Mileage' then ch.chra_value else null end) as Time_and_Mileage
, sum(case when chrg_level1_care = 'Surcharges' then ch.chra_value else null end) as Surcharges
, sum(case when chrg_level2_clev = 'Extras' then ch.chra_value else null end) as Extras
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
    else 'Franchise' 
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
--    and rntl_driver_origin in ('DE','AT','CH','FR','MC','GB','IT','ES','BE','NL','LU','US','CA') 
    and year(b.date_date) in (2023,2022,2019)
    and a.rntl_mvnr = 9497670359
order by date_date)

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
, a.total_ndat_corporate_revenue_euro
, case when a.row_count = 1 then ch.total_charges else null end as total_charges
, case when a.row_count = 1 then ch.Time_and_Mileage else null end as Time_and_Mileage
, case when a.row_count = 1 then ch.Surcharges else null end as Surcharges
, case when a.row_count = 1 then ch.Extras else null end as Extras
, 1.0*ch.total_charges * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro as bound_total_charges
, 1.0*ch.Time_and_Mileage * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro as bound_Time_and_Mileage
, 1.0*ch.Surcharges * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro as bound_Surcharges
, 1.0*ch.Extras * ndat_corporate_revenue_euro/total_ndat_corporate_revenue_euro as bound__Extras
from initial_pull a
left join charges ch on ch.chra_mvnr = a.rntl_mvnr
    and ch.chra_mser = a.rntl_mser
    and ch.chra_konr = a.rntl_konr
