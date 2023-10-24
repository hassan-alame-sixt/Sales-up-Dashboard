with initial_pull as (
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

, intermediary_pull as (
select 
  p.*
, i.account_owner_type
, i.acquired_by_rent_id
, i.acquired_rent_date
, i.acquired_by_rent_region
, i.inside_sales_person_region
, i.acquired_by_rent_saleschannel
, i.deal_amount
, i.total_sixt_potential
, i.total_customer_spend
from "customer_shop"."pa_dim_partners" p
left join initial_pull i on i.prtn_kdnr = p.prtn_kdnr and i.rn = 1 
)

/*
with initial_pull as (
select 
  "meta.timestamp"
, "company_data.customer_number" as prtn_kdnr
, "company_data.company_name"
, "company_data.first_level_parent" 
, "company_data.second_level_parent"
, "company_data.customer_classification"
, "company_data.customer_group_code"
, "inside_sales.account_owner_id"
, "inside_sales.inside_salesperson_id"
, "inside_sales.account_owner_type" as account_owner_type --2
, "inside_sales.acquired_by_rent_id" as acquired_by_rent_id --3
, "inside_sales.acquired_rent_date" as acquired_rent_date --4
, "inside_sales.acquired_by_rent_region" as acquired_by_rent_region --5
, "potential_information.deal_amount" as deal_amount --6
, "inside_sales.acquired_by_rent_saleschannel" as acquired_by_rent_saleschannel --7
, "potential_information.total_sixt_potential_without_row" as total_sixt_potential --8
, "potential_information.total_customer_spend_without_row" as total_customer_spend --9
, "inside_sales.account_owner_region" as account_owner_region --10
, "company_data.status_code" as status_code --11
, row_number() over (partition by "company_data.customer_number" order by "meta.timestamp" desc) as rn
from "sds_prod_ingestion_store_public_datalake"."customerorgv1" c)
*/
