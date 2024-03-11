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
from  "rent_shop"."ra_fct_subscriptions" r 
Left Join "common_shop"."ge_dim_dates" g on (Cast(g.date_date as date) between Cast(rnsb_handover_date as date) and Cast(rnsb_return_date as date))
left Join "common_shop"."ge_dim_daily_exchange_rates" gd On (Replace(r.rnsb_currency_code,'CHF','SFR') = gd.exrd_foreign_currency_code)
Where exrd_rate_date = date_trunc('month', Cast(g.date_date as date))
) 

, processed_unlimited as (
select 
rnsb_aonr
, sum(Revenue_per_day) as rpds
from sixt_unlimited 
group by 1
)

, invoices as (
select sbin_subscription_num as rnsb_aonr
, sum(sbin_net_amount) as net_sum
from "rent_shop"."ra_fct_subscription_invoices"
group by 1 
)

select 
 u.rnsb_aonr
, u.rpds
, i.net_sum
from processed_unlimited u
left join invoices i on i.rnsb_aonr = u.rnsb_aonr;

select sum (net_sum) from invoices--The numbers are so close 
union all 
select sum(rpds) from processed_unlimited


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
from  "rent_shop"."ra_fct_subscriptions" r 
Left Join "common_shop"."ge_dim_dates" g on (Cast(g.date_date as date) between Cast(rnsb_handover_date as date) and Cast(rnsb_return_date as date))
left Join "common_shop"."ge_dim_daily_exchange_rates" gd On (Replace(r.rnsb_currency_code,'CHF','SFR') = gd.exrd_foreign_currency_code)
Where exrd_rate_date = date_trunc('month', Cast(g.date_date as date))
) 

, processed_unlimited as (
select 
rnsb_aonr
, sum(Revenue_per_day) as rpds
from sixt_unlimited 
group by 1
)

, invoices as (
select sbin_subscription_num as rnsb_aonr
, sum(sbin_net_amount) as net_sum
from "rent_shop"."ra_fct_subscription_invoices"
group by 1 
)

, intermediat as (
select 
 u.rnsb_aonr
, u.rpds
, i.net_sum
, case when abs(i.net_sum - u.rpds)/u.rpds >0.01 then 1 else 0 end as Noticeable_Difference
from processed_unlimited u
left join invoices i on i.rnsb_aonr = u.rnsb_aonr
)

select count(*), sum(Noticeable_Difference) from intermediat 
--40% of rows haee more than 1% of difference 
--31% of rows have more than 5% of difference 
--23% have more than 10% difference


--Accesss Management
--Unlimited 

select * from "rent_shop"."ra_fct_subscription_invoices" i limit 100
select * from "rent_shop"."ra_fct_subscriptions" limit 100
select distinct sys_taken_datm from "rent_shop"."ra_fct_subscription_invoices"
select distinct rnsb_status_code, rnsb_status from "rent_shop"."ra_fct_subscriptions" 
select distinct sys_actual_flg, sys_deleted_flg from "rent_shop"."ra_fct_subscriptions" 
select distinct sys_taken_datm from "rent_shop"."ra_fct_subscriptions" 

select 
  count(*)
, count(distinct sbin_invoice_num)
, sum(case when r.rnsb_aonr is null then 1 else 0 end) -- adding  r.prtn_kdnr = i.prtn_kdnr into where clause increases mismatch from 0 to 1793 rows and r.mndt_code = i.mndt_code increases it to 1807
from "rent_shop"."ra_fct_subscription_invoices" i 
left join "rent_shop"."ra_fct_subscriptions" r on r.rnsb_aonr = i.sbin_subscription_num --and r.prtn_kdnr = i.prtn_kdnr and r.mndt_code = i.mndt_code
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = i.prtn_kdnr 

select count(*) --826 have completely different kdnr mentioned 
from "rent_shop"."ra_fct_subscription_invoices" i 
left join "rent_shop"."ra_fct_subscriptions" r on r.rnsb_aonr = i.sbin_subscription_num --and r.prtn_kdnr = i.prtn_kdnr and r.mndt_code = i.mndt_code
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = i.prtn_kdnr 
left join "customer_shop"."pa_dim_partners" pa2 on pa2.prtn_kdnr = r.prtn_kdnr 
where r.prtn_kdnr != i.prtn_kdnr and pa2.prtn_parent_calc_num != pa.prtn_parent_calc_num 

select -- Currency differnce is often with sbin_currency = 'SFR' otherwise we have only 54 rows where EURO is not translated 
  i.sbin_invoice_num 
, i.prtn_kdnr
, r.prtn_kdnr as sbuscription_prtn_kdnr
, pa.prtn_parent_calc_num
, pa2.prtn_parent_calc_num as r_prtn_parent_calc_num 
, i.mndt_code
, i.sbin_item_num
, i.sbin_subscription_num --Same as rnsb_aonr ? 
, r.rnsb_aonr
, i.sbin_booking_date
, i.sbin_net_amount
, i.sbin_vat_amount
, i.sbin_tax_percent
, i.sbin_gross_amount
, i.sbin_currency
, r.rnsb_currency_code
, r.rnsb_handover_date
, r.rnsb_return_date
, r.rnsb_month_rate
, r.vhcl_group
from "rent_shop"."ra_fct_subscription_invoices" i 
left join "rent_shop"."ra_fct_subscriptions" r on r.rnsb_aonr = i.sbin_subscription_num --and r.prtn_kdnr = i.prtn_kdnr and r.mndt_code = i.mndt_code
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = i.prtn_kdnr 
left join "customer_shop"."pa_dim_partners" pa2 on pa2.prtn_kdnr = r.prtn_kdnr 
where i.sbin_currency != r.rnsb_currency_code and 	
sbin_currency != 'SFR' --r.prtn_kdnr != i.prtn_kdnr and pa2.prtn_parent_calc_num != pa.prtn_parent_calc_num 

select 
  i.sbin_invoice_num 
, i.prtn_kdnr
, r.prtn_kdnr as sbuscription_prtn_kdnr
, pa.prtn_parent_calc_num
, pa2.prtn_parent_calc_num as r_prtn_parent_calc_num 
, i.mndt_code
, i.sbin_item_num
, i.sbin_subscription_num --Same as rnsb_aonr ? 
, r.rnsb_aonr
, i.sbin_booking_date
, i.sbin_net_amount
, i.sbin_vat_amount
, i.sbin_currency
, i.sbin_tax_percent
, i.sbin_gross_amount
, i.sys_taken_datm
, r.rnsb_handover_date
, r.rnsb_return_date
, r.rnsb_month_rate
, r.rnsb_currency_code
, r.vhcl_group
select count(*)
from "rent_shop"."ra_fct_subscription_invoices" i 
left join "rent_shop"."ra_fct_subscriptions" r on r.rnsb_aonr = i.sbin_subscription_num --and r.prtn_kdnr = i.prtn_kdnr and r.mndt_code = i.mndt_code
left join "customer_shop"."pa_dim_partners" pa on pa.prtn_kdnr = i.prtn_kdnr 
left join "customer_shop"."pa_dim_partners" pa2 on pa2.prtn_kdnr = r.prtn_kdnr 
where r.prtn_kdnr != i.prtn_kdnr and pa2.prtn_parent_calc_num != pa.prtn_parent_calc_num 


  i.sbin_invoice_num
, i.prtn_kdnr
, i.mndt_code
, i.sbin_item_num
, i.sbin_subscription_num
, i.sbin_booking_date
, i.sbin_net_amount
, i.sbin_vat_amount
, i.sbin_currency
, i.sbin_tax_percent
, i.sbin_gross_amount
, i.sys_actual_flg
, i.sys_deleted_flg
, i.rnsb_aonr
, i.mndt_code
, i.rnsb_handover_date
, i.rnsb_return_date
, i.rnsb_status
, i.rnsb_month_rate
, i.rnsb_currency_code
, i.vhcl_group


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
from "rent_shop"."ra_fct_subscription_invoices" i
left join "rent_shop"."ra_fct_subscriptions" r  on r.rnsb_aonr = i.sbin_subscription_num 
Left Join "common_shop"."ge_dim_dates" g on (Cast(g.date_date as date) between Cast(rnsb_handover_date as date) and Cast(rnsb_return_date as date))
left Join "common_shop"."ge_dim_daily_exchange_rates" gd On (Replace(r.rnsb_currency_code,'CHF','SFR') = gd.exrd_foreign_currency_code)
Where exrd_rate_date = date_trunc('month', Cast(g.date_date as date))
) 

"Unlimited +" as Product
, Null as rsrv_resn
, Null as rsrv_new_customer
, Null as rsrv_status
, Null as rsrv_status_extended
, Null as rsrv_source_chl1
, Null as rsrv_source_chl2
, Null as rsrv_source_chl3
, Null asrsrv_yield_source
, Null as rsrv_yield_source_level2
, Null as rsrv_yield_source_level3
, Null as rsrv_posl_country_code
, Null as rntl_mvnr
, Null as rntl_mser
, Null as rntl_konr
, rnsb_aonr as rnsb_aonr --New
, brnc_code_handover
, brnc_name
, brnc_operator
, brnc_main_type
, brnc_type_code
, brnc_type
, brnc_pool_code
, brnc_pool_name
, brnc_continent
, brnc_country_code_iso
, brnc_country
, brnc_active_flg
, brnc_corporate_franchise
, brnc_country_region
, brnc_country_region_franchise_breakdown
, brnc_region
, brnc_state
, brnc_city
, cleansed_dfr
, cleansed_dfi
, cstm_kdnr
abkz
cstm_name
kdnr_prt_bed
kdnr_bed_vkni
kdnr_highest_person_region
kdnr_highest_working_channel
kdnr_prtn_person_region
kdnr_prtn_working_channel
kdnr_account_owner
kdnr_highest_linked
dto
dto_name
dfi
dfi_name
prtn_parent_domestic_country
sf_account_owner_type
sf_acquired_by_rent_id
sf_acquired_rent_date
sf_acquired_by_rent_region
sf_inside_sales_person_region
sf_acquired_by_rent_saleschannel
sf_deal_amount
sf_total_sixt_potential
sf_total_customer_spend
agnc_age
age_name
age_prt_bed
age_highest_person_region
age_highest_working_channel
age_prtn_person_region
age_prtn_working_channel
age_account_owner
age_highest_linked
dtt
dtt_name
dfr
dfr_name
rntl_type_code
rntl_type
rntl_payment_type
internet_reservation_resn
app_reservation_resn
rsrv_cancelled_resn
rsrv_noshow_resn
internet_reservation_mvnr
app_reservation_mvnr
rntl_one_way_mvnr
rntl_correction_mvnr
domestic_country_mvnr
rntl_bev_mvnr
cstm_account_manager_num
cstm_account_manager_name
rsrv_date
rntl_handover_datm
rntl_return_datm
rntl_accounting_date
vhgr_crs
vhcl_group
vhcl_checked_out_group
vhcl_type
vhcl_category_level1
vhcl_category_level2
vhcl_category_level3
vhcl_category_level4
vhcl_owner_status
vhat_elty
rate_id
rate_prl
rate_bundle
rate_type
rate_designation
rate_type_level1_gare
rate_type_level2_glev
rate_type_level3_aknm
rate_type_level4_aktv
AKTV
rate_crm_type_gare_clv
rate_gdat
rate_vdat
rate_next_gdat
rate_validity_end
product_level1_source
product_level2_car_truck
product_level2_cnb_vnt
product_level3_name
chra_pos
chra_inty
chra_chco
chrg_name
rsts_excitement_num
rsts_recommendation_num
rntl_revenue
rntl_discount
rental_days
advanced_booking
chra_value
Time_and_Mileage
improved_revenue
Inc_tot
internet_reservation_revenue
app_reservation_revenue
domestic_revenue
non_domestic_revenue