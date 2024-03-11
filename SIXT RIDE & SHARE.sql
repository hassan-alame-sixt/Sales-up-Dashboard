/* The following Variables are only mentioned for reference related to source code. 
   They are obtained from the following Github Link: https://github.com/Sixt/com.sixt.lib.sds-airflow-dags/blob/develop/athena_etl/athena_etl_packages/events_etl/current_variables.inc

# FORMERLY "DWH_SHARE"
CONST DB_DWH_SHARE_CURRENT="sds_${AWS_ENV}_share_dwh_current"

# FORMERLY "DWH_SHARE_HISTORY"
CONST DB_DWH_SHARE_HISTORY="sds_${AWS_ENV}_share_dwh_history"

# FORMERLY "DWH_RIDE"
CONST DB_DWH_RIDE="sds_${AWS_ENV}_ride_dwh"
*/

with get_ride_data as 
/*
It is its own company that is somewhat independent from Sixt 
KDNR will not align with customer company 
It's possible to have several KDNRs connected to the same company 

*/
(--Extracted from line 1133 from https://github.com/Sixt/com.sixt.lib.sds-airflow-dags/blob/develop/athena_etl/athena_etl_packages/events_etl/sa_fct_b2b_bonus_calculations/sa_fct_b2b_bonus_calculations.sql#L623
    Select 
          rme.id as ride_id,
          CASE WHEN rme.sixt_customer_number IS NOT NULL THEN rme.sixt_customer_number 
           WHEN rme.profile_type = 'CORPORATE' THEN cast(cc.sixt_customer_number AS INT) END AS kdnr, -- KDNR nach QllikView definition
          rme.lastmodifiedaccountingdate_utc as ride_accounting_date,
          cast(rme.ridestartdate as timestamp) as dstartdate, 
          cast(rme.rideenddate as timestamp) as rideenddate, 
          rme.start_country,
          rme.actual_ride, 
          rme.status,
          rme.product,
          rme.customer_vat,
          rme.destination_address,
          rme.destination_airport_yn,
          rme.eur_adjusted_ride_price
    from "sds_prod_ride_dwh"."reservations_main_extended" rme 
    Left JOIN "sds_prod_ride_dwh"."customer_company" cc ON cc.customer_company_id = rme.customer_company_id 
	Where rme.actual_ride = 1
/* Ride doesn't have actual KDNRs 37956 entries don't even match 

select 
  count(*)
, count(distinct parent_customer_company_id)
, sum(case when pa.prtn_kdnr is not null then 1 else 0 end)
, sum(case when parent_customer_company_id = prtn_parent_calc_num then 1 else 0 end)
from "sds_prod_ride_dwh"."customer_company" cc
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = cc.customer_company_id 	
--46747, 37490, 8791, 2399

select count(*) --2399
from "sds_prod_ride_dwh"."customer_company" cc
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = cc.customer_company_id
where pa.prtn_kdnr is not null and parent_customer_company_id = prtn_parent_calc_num
*/

)

, bonus_schemes as 
(--Extracted from line 617 from https://github.com/Sixt/com.sixt.lib.sds-airflow-dags/blob/develop/athena_etl/athena_etl_packages/events_etl/sa_fct_b2b_bonus_calculations/sa_fct_b2b_bonus_calculations.sql#L623
    
                select '00005357' as sfbs_number            -- 2021/2022        https://sixt.my.salesforce.com/aAK3V000000bllYWAQ
        UNION   select '00005358'                           -- 2022/2023        https://sixt.my.salesforce.com/aAK3V000000bllZWAQ
    )
    
, get_join_kdnrs_for_sfbs as
(--Extracted from line 1086 from https://github.com/Sixt/com.sixt.lib.sds-airflow-dags/blob/develop/athena_etl/athena_etl_packages/events_etl/sa_fct_b2b_bonus_calculations/sa_fct_b2b_bonus_calculations.sql#L623
    select distinct sfbs_timestamp,
                    cstm_kdnr,
                    sfbs_number,
            	    sfbs_name,
            	    sfbs_barcode_id, 
            	    sfbs_account_kdnr,
            	    sfbs_account_name,
            	    sfbs_including_kdnrs,
            	    sfbs_excluding_kdnrs,
            	    sfbs_status,
                    sfbs_valid_from,
                    sfbs_valid_to,
                    sfbs_turnover_thresholds_car, 
                	sfbs_turnover_thresholds_truck,
                    sfbs_calculation_base_vs_threshold,
                    sfbs_calculation_base_car,
                    sfbs_calculation_base_truck,
                    sfbs_bonus_in,
                    sfbs_bonus_countries_codes,
                    sfbs_type_of_threshold,
                    sfbs_bonus_on,
                    sfbs_turnover,
                    sfbs_sixthq,
                    sixthq_iso,
                    sixthq_tax_rate,
                    sfbs_created,
                    sfbs_created_by,
                    sfbs_last_modified,
                    sfbs_last_modified_by,
                    sfbs_type,
                    sfbs_bonus_payment,
                    sfbs_offer_type,
                    sfbs_bonus_calculated,
                    sfbs_status_delete_flg,
                    sfbs_status_forecast_flg,
                    sfbs_status_final_flg,
                    sfbs_status_name,
                    sfbs_bonus_category,
		            sfbs_total_days,
		            sfbs_current_day,
	                sfbs_forecast_factor,
	                sfbs_bonus_scheme_thresholds_id
    from "sales_shop"."sa_fct_b2b_customer_bonus_raw_rentals" rs where rs.sfbs_number IN  (select sfbs_number from bonus_schemes)
    )

, get_share_data as --Contact Lukas Wolf 
--Business Owner is Christina Ott
(--Extracted from line 1155 from https://github.com/Sixt/com.sixt.lib.sds-airflow-dags/blob/develop/athena_etl/athena_etl_packages/events_etl/sa_fct_b2b_bonus_calculations/sa_fct_b2b_bonus_calculations.sql#L623
     select 
      rs.*
    , jrny_journey_id
    , jrdt_permission_country AS permission_country
    , brnc_name_start AS branch_start
    , jrdt_local_start_date AS start_date
    , jrdt_local_end_date AS end_date
    , jrdt_journey_duration_minutes AS jrny_dur_min
    , jrin_invoice_id
    , a.onpr_person_id_active
    , a.onpp_profile_id
    , b.prtn_kdnr_cdnr
    , jrin_total_net
    , jrit_net_price_rate
    , jrny_source
    , brnc_name_start
    , jrdt_permission_country
    , onpp_corp_customer_added_datm  
    , d.prtn_highest_person_region 
    , d.prtn_parent_calc_num
    , d.prtn_person_region
    , d.prtn_registration_range_code
    , d.prtn_parent_name
    , d.prtn_parent_num
    , d.prtn_subsidiary_name
    , d.prtn_subsidiary_num
    , d.prtn_name 
    from share_shop.on_fct_journey_details a 
    left join  (select onpr_person_id
                      , onpp_profile_id
                      , prtn_kdnr_cdnr
                      , onpp_corp_customer_added_datm        
                from "sds_prod_share_dwh_current".pa_dim_one_person_profiles
                where prtn_kdnr_cdnr is not null ) as b 
    on a.onpr_person_id_active = b.onpr_person_id
    and a.onpp_profile_id = b.onpp_profile_id
    left join (select   prtn_kdnr
                      , prtn_name 
                      , prtn_subsidiary_num 
                      , prtn_subsidiary_name
                      , prtn_parent_num
                      , prtn_parent_name
                      , prtn_registration_range_code
                      , prtn_person_region
                      , prtn_parent_calc_num
                      , prtn_highest_person_region 
                from "customer_shop"."pa_dim_partners") as d 
                on d.prtn_kdnr = b.prtn_kdnr_cdnr
    left join get_join_kdnrs_for_sfbs rs ON (rs.cstm_kdnr = d.prtn_kdnr) 
    where jrdt_journey_flg = 1 
    and d.prtn_kdnr is not null 
  --  and rs.sfbs_number IN (select sfbs_number from bonus_schemes)
    and DATE(jrdt_local_end_date)  BETWEEN DATE(rs.sfbs_valid_from) AND DATE(rs.sfbs_valid_to)  
    and jrin_total_net is not null
    and jrdt_permission_country = 'GERMANY'
    )

    /* 
    -- Ride Data 
    -- Rajib Kar
    -- Karol Kuhl
select 
  count(*)
, count(distinct parent_customer_company_id) --This is not a KDNR 
, sum(case when pa.prtn_kdnr is not null then 1 else 0 end)
, sum(case when parent_customer_company_id = prtn_parent_calc_num then 1 else 0 end)
from "sds_prod_ride_dwh"."customer_company" cc
left join "customer_shop"."pa_dim_partners" pa on cast(pa.prtn_kdnr as varchar) = cc.sixt_customer_number --This is not KDNR rather this is sixt_customer_number

select * from "sds_prod_ride_dwh"."customer_company" cc where sixt_customer_number is not null limit 100 --Note there can be more than one KDNR mapped to a single customer ID.
--There 1 leading KDNR which should map 1 to 1 to ours

select count(*), sum(case when sixt_customer_number is not null then 1 else 0 end) from "sds_prod_ride_dwh"."customer_company"
--The mismatch should be handled 

select customer_company_id, customer_company_id, 
last_ride_date
 
 select count(*)
  from "sds_prod_ride_dwh"."customer_company" where  sixt_customer_number is null  
  and year(	
last_ride_date) >= 2019 limit 1000
*/