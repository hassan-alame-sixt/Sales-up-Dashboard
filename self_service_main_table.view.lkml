# The name of this view in Looker is "Self Service Main Table"
view: self_service_main_table {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: sales_mart.self_service_main_table ;;
  suggestions: yes

  # No primary key is defined for this view. In order to join this view in an Explore,
  # define primary_key: yes on a dimension that has no repeated values.

    # Here's what a typical dimension looks like in LookML.
    # A dimension is a groupable field that can be used to filter query results.
    # This dimension will be called "Age Account Owner" in Explore.

  dimension: age_account_owner {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_account_owner ;;
  }

  dimension: age_highest_linked {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_highest_linked ;;
  }

  dimension: age_prt_bed {
    view_label: "AGE Columns"
    type: number
    sql: ${TABLE}.age_prt_bed ;;
  }

  dimension: agnc_age {
    view_label: "AGE Columns"
    type: number
    sql: ${TABLE}.agnc_age ;;
  }

  dimension: age_name {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_name ;;
  }

  dimension: brnc_city {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_city ;;
  }

  dimension: brnc_code_handover {
    view_label: "Branch Columns"
    type: number
    sql: ${TABLE}.brnc_code_handover ;;
  }

  dimension: brnc_continent {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_continent ;;
  }

  dimension: brnc_corporate_franchise {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_corporate_franchise ;;
  }

  dimension: brnc_country {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_country ;;
  }

  dimension: brnc_country_code_iso {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_country_code_iso ;;
  }

  dimension: brnc_country_region {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_country_region ;;
  }

  dimension: brnc_main_type {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_main_type ;;
  }

  dimension: brnc_name {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_name ;;
  }

  dimension: brnc_operator {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_operator ;;
  }

  dimension: brnc_pool_code {
    view_label: "Branch Columns"
    type: number
    sql: ${TABLE}.brnc_pool_code ;;
  }

  dimension: brnc_pool_name {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_pool_name ;;
  }

  dimension: brnc_region {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_region ;;
  }

  dimension: brnc_state {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_state ;;
  }

  dimension: brnc_type {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_type ;;
  }

  dimension: chra_chco {
    view_label: "Charge Code Columns"
    type: string
    sql: ${TABLE}.chra_chco ;;
  }

  dimension: chra_inty {
    view_label: "Charge Code Columns"
    type: string
    sql: ${TABLE}.chra_inty ;;
  }

  dimension: chra_value {
    view_label: "Charge Code Columns"
    type: number
    sql: ${TABLE}.chra_value ;;
  }

  dimension: chrg_name {
    view_label: "Charge Code Columns"
    type: string
    sql: ${TABLE}.chrg_name ;;
  }

  dimension: cleansed_dfi {
    view_label: "KDNR Columns"
    type: yesno
    sql: ${TABLE}.cleansed_dfi ;;
  }

  dimension: cleansed_dfr {
    view_label: "AGE Columns"
    type: yesno
    sql: ${TABLE}.cleansed_dfr ;;
  }

  dimension: cstm_kdnr {
    view_label: "KDNR Columns"
    type: number
    sql: ${TABLE}.cstm_kdnr ;;
  }

  dimension: cstm_name {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.cstm_name ;;
  }

  dimension: dfi {
    view_label: "KDNR Columns"
    type: number
    sql: ${TABLE}.dfi ;;
  }

  dimension: dfi_name {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.dfi_name ;;
  }

  dimension: dfr {
    view_label: "AGE Columns"
    type: number
    sql: ${TABLE}.dfr ;;
  }

  dimension: dfr_name {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.dfr_name ;;
  }

  dimension: dto {
    view_label: "KDNR Columns"
    type: number
    sql: ${TABLE}.dto ;;
  }

  dimension: dto_name {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.dto_name ;;
  }

  dimension: dtt {
    view_label: "AGE Columns"
    type: number
    sql: ${TABLE}.dtt ;;
  }

  dimension: dtt_name {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.dtt_name ;;
  }

  dimension: improved_revenue {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.improved_revenue ;;
  }

  # A measure is a field that uses a SQL aggregate function. Here are defined sum and average
  # measures for this dimension, but you can also add measures of many different aggregates.
  # Click on the type parameter to see all the options in the Quick Help panel on the right.

  measure: total_improved_revenue {
    view_label: "Measures"
    type: sum
    sql: ${improved_revenue} ;;
    value_format: "€0.00"}

  measure: average_improved_revenue {
    view_label: "Measures"
    type: average
    sql: ${improved_revenue} ;;
    value_format: "€0.00"}

  dimension: kdnr_account_owner {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.kdnr_account_owner ;;
  }

  dimension: kdnr_highest_linked {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.kdnr_highest_linked ;;
  }

  dimension: kdnr_prt_bed {
    view_label: "KDNR Columns"
    type: number
    sql: ${TABLE}.kdnr_prt_bed ;;
  }

  dimension: rate_id {
    view_label: "Rate Columns"
    type: number
    sql: ${TABLE}.rate_id ;;
  }

  dimension: rate_prl {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_prl ;;
  }

  dimension: rate_type_level1_gare {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_type_level1_gare ;;
  }

  dimension: rate_type_level2_glev {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_type_level2_glev ;;
  }

  dimension: rate_type_level3_aknm {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_type_level3_aknm ;;
  }

  dimension: rate_type_level4_aktv {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_type_level4_aktv ;;
  }

  dimension: AKTV {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.AKTV ;;
  }
  # Dates and timestamps can be represented in Looker using a dimension group of type: time.
  # Looker converts dates and timestamps to the specified timeframes within the dimension group.

  dimension_group: rsrv_date {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rsrv_date ;;
  }

  dimension_group: rntl_handover_datm {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rntl_handover_datm ;;
  }

  dimension_group: rntl_return_datm {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rntl_return_datm ;;
  }

  dimension_group: rntl_accounting_date {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rntl_accounting_date ;;
  }

  dimension: rntl_discount {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.rntl_discount ;;
  }

  measure: total_rntl_discount {
    view_label: "Measures"
    type: sum
    sql: ${rntl_discount} ;;
    value_format: "€0.00"}

  measure: average_rntl_discount {
    view_label: "Measures"
    type: average
    sql: ${rntl_discount} ;;
    value_format: "€0.00"}

  dimension: rntl_konr {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rntl_konr ;;
  }

  dimension: rntl_mser {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rntl_mser ;;
  }

  dimension: rntl_mvnr {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rntl_mvnr ;;
  }

  dimension: rntl_revenue {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.rntl_revenue ;;
  }

  dimension: advanced_booking {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.advanced_booking ;;
  }

  dimension: rental_days {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.rental_days ;;
  }

  measure: total_rntl_revenue {
    view_label: "Measures"
    type: sum
    sql: ${rntl_revenue} ;;
    value_format: "€0.00"}

  measure: average_rntl_revenue {
    view_label: "Measures"
    type: average
    sql: ${rntl_revenue} ;;
    value_format: "€0.00"}


  measure: total_rental_days {
    view_label: "Measures"
    type: sum
    sql: ${rental_days} ;;}


  measure: average_rental_days {
    view_label: "Measures"
    type: average
    sql: ${rental_days} ;;}


  measure: total_advanced_booking {
    view_label: "Measures"
    type: sum
    sql: ${advanced_booking} ;;}


  measure: average_advanced_booking {
    view_label: "Measures"
    type: average
    sql: ${advanced_booking} ;;}


  dimension: rntl_type {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.rntl_type ;;
  }

  dimension: rntl_type_code {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.rntl_type_code ;;
  }

  dimension: rntl_payment_type {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.rntl_payment_type ;;
  }

  dimension: product_level2_car_truck {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.product_level2_car_truck ;;
  }

  dimension: product_level3_long_short {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.product_level3_long_short ;;
  }

  dimension: rsrv_cancelled_flg {
    view_label: "Reservation Columns"
    type: number
    sql: ${TABLE}.rsrv_cancelled_flg ;;
  }

  dimension: rsrv_noshow_flg {
    view_label: "Reservation Columns"
    type: number
    sql: ${TABLE}.rsrv_noshow_flg ;;
  }

  dimension: rsrv_resn {
    view_label: "Reservation Columns"
    type: number
    sql: ${TABLE}.rsrv_resn ;;
  }

  dimension: rsrv_source_chl1 {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_source_chl1 ;;
  }

  dimension: rsrv_source_chl2 {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_source_chl2 ;;
  }

  dimension: rsrv_source_chl3 {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_source_chl3 ;;
  }

  dimension: rsrv_status {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_status ;;
  }

  dimension: rsrv_status_extended {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_status_extended ;;
  }

  dimension: rsrv_yield_source {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_yield_source ;;
  }

  dimension: rsrv_yield_source_level2 {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_yield_source_level2 ;;
  }

  dimension: rsrv_yield_source_level3 {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_yield_source_level3 ;;
  }

  dimension: rsrv_posl_country_code {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_posl_country_code ;;
  }

  dimension: cstm_account_manager_num {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.cstm_account_manager_num ;;
  }

  dimension: cstm_account_manager_name {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.cstm_account_manager_name ;;
  }

  dimension: vhat_elty {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhat_elty ;;
  }

  dimension: vhcl_category_level1 {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_category_level1 ;;
  }

  dimension: vhcl_category_level2 {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_category_level2 ;;
  }

  dimension: vhcl_category_level3 {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_category_level3 ;;
  }

  dimension: vhcl_category_level4 {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_category_level4 ;;
  }

  dimension: vhcl_checked_out_group {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_checked_out_group ;;
  }

  dimension: vhcl_group {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_group ;;
  }

  dimension: vhcl_owner_status {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_owner_status ;;
  }

  dimension: vhcl_type {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhcl_type ;;
  }

  dimension: vhgr_crs {
    view_label: "Vehicle Columns"
    type: string
    sql: ${TABLE}.vhgr_crs ;;
  }

  measure: total_mvnrs {
    view_label: "Measures"
    type: count_distinct
    sql: ${rntl_mvnr} ;;
  }

  measure: total_reservations {
    view_label: "Measures"
    type: count_distinct
    sql: ${rsrv_resn} ;;
  }

  measure: total_chco {
    view_label: "Measures"
    type: count
    drill_fields: [detail*]
  }

  measure: RPD {
    view_label: "Measures"
    type: number
    sql: 1.0*${total_improved_revenue}/${total_rental_days} ;;
    value_format: "€0.00"
  }

  dimension: ytd_only {
    group_label: "To-Date Filters"
    label: "YTD"
    view_label: "_PoP"
    type: yesno
    sql:  (EXTRACT(DOY FROM ${TABLE}.rntl_accounting_date) < EXTRACT(DOY FROM current_date)
                    OR
                (EXTRACT(DOY FROM ${TABLE}.rntl_accounting_date) = EXTRACT(DOY FROM current_date) AND
                EXTRACT(HOUR FROM ${TABLE}.rntl_accounting_date) < EXTRACT(HOUR FROM current_date))
                    OR
                (EXTRACT(DOY FROM ${TABLE}.rntl_accounting_date) = EXTRACT(DOY FROM current_date) AND
                EXTRACT(HOUR FROM ${TABLE}.rntl_accounting_date) <= EXTRACT(HOUR FROM current_date) AND
                EXTRACT(MINUTE FROM ${TABLE}.rntl_accounting_date) < EXTRACT(MINUTE FROM current_date)))  ;;
  }



  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
  brnc_name,
  brnc_pool_name,
  dto_name,
  dfi_name,
  dtt_name,
  dfr_name,
  chrg_name
  ]
  }

}
