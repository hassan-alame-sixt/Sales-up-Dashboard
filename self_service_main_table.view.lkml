# The name of this view in Looker is "Self Service Main Table 2"

view: sales_up_main_table {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: sales_mart.self_service_main_table ;;
  suggestions: yes

  # No primary key is defined for this view. In order to join this view in an Explore,
  # define primary_key: yes on a dimension that has no repeated values.

  # Here's what a typical dimension looks like in LookML.
  # A dimension is a groupable field that can be used to filter query results.
  # This dimension will be called "Advanced Booking" in Explore.

  dimension: advanced_booking {
    view_label: "Measures"
    type: number
    sql: ${TABLE}.advanced_booking ;;
  }

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

  dimension: age_highest_person_region {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_highest_person_region ;;
  }

  dimension: age_highest_working_channel {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_highest_working_channel ;;
  }

  dimension: age_name {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_name ;;
  }

  dimension: age_prt_bed {
    view_label: "AGE Columns"
    type: number
    sql: ${TABLE}.age_prt_bed ;;
  }

  dimension: age_prtn_person_region {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_prtn_person_region ;;
  }

  dimension: age_prtn_working_channel {
    view_label: "AGE Columns"
    type: string
    sql: ${TABLE}.age_prtn_working_channel ;;
  }

  dimension: agnc_age {
    view_label: "AGE Columns"
    type: number
    sql: ${TABLE}.agnc_age ;;
  }

  dimension: aktv {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.aktv ;;
  }

  dimension: app_reservation {
    view_label: "Reservation Columns"
    type: yesno
    sql: ${TABLE}.app_reservation ;;
  }

  dimension: app_reservation_revenue {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: case when ${TABLE}.app_reservation is True then ${TABLE}.improved_revenue else null end;;
  }

  dimension: app_reservation_mvnr {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: case when ${TABLE}.app_reservation is True then ${TABLE}.rntl_mvnr else null end;;
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

  dimension: brnc_country_region_franchise_breakdown {
    view_label: "Branch Columns"
    type: string
    sql: ${TABLE}.brnc_country_region_franchise_breakdown ;;
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

  dimension: chra_pos {
    view_label: "Charge Code Columns"
    type: number
    sql: ${TABLE}.chra_pos ;;
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

  dimension: cstm_account_manager_name {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.cstm_account_manager_name ;;
  }

  dimension: cstm_account_manager_num {
    view_label: "KDNR Columns"
    type: number
    sql: ${TABLE}.cstm_account_manager_num ;;
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

  dimension: inc_tot {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.inc_tot ;;
  }

  dimension: internet_reservation {
    view_label: "Reservation Columns"
    type: yesno
    sql: ${TABLE}.internet_reservation ;;
  }

  dimension: internet_reservation_revenue {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: case when ${TABLE}.internet_reservation is True then ${TABLE}.improved_revenue else null end;;
  }

  dimension: internet_reservation_mvnr {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: case when ${TABLE}.internet_reservation is True then ${TABLE}.rntl_mvnr else null end;;
  }

  dimension: is_domestic_country {
    view_label: "KDNR Columns"
    type: yesno
    sql: ${TABLE}.is_domestic_country ;;
  }

  dimension: domestic_revenue {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: case when ${TABLE}.is_domestic_country is True then ${TABLE}.improved_revenue else 0 end;;
  }

  dimension: non_domestic_revenue {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: case when ${TABLE}.is_domestic_country is False then ${TABLE}.improved_revenue else 0 end;;
  }

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

  dimension: kdnr_highest_person_region {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.kdnr_highest_person_region ;;
  }

  dimension: kdnr_highest_working_channel {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.kdnr_highest_working_channel ;;
  }

  dimension: kdnr_prt_bed {
    view_label: "KDNR Columns"
    type: number
    sql: ${TABLE}.kdnr_prt_bed ;;
  }

  dimension: kdnr_prtn_person_region {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.kdnr_prtn_person_region ;;
  }

  dimension: kdnr_prtn_working_channel {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.kdnr_prtn_working_channel ;;
  }

  dimension: mvnr_count {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.mvnr_count ;;
  }

  dimension: product_level1_source {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.product_level1_source ;;
  }

  dimension: product_level2_car_truck {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.product_level2_car_truck ;;
  }

  dimension: product_level2_cnb_vnt {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.product_level2_cnb_vnt ;;
  }

  dimension: product_level3_name {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.product_level3_name ;;
  }

  dimension: prtn_parent_domestic_country {
    view_label: "KDNR Columns"
    type: string
    sql: ${TABLE}.prtn_parent_domestic_country ;;
  }

  dimension: rate_bundle {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_bundle ;;
  }

  dimension: rate_crm_type_gare_clv {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_crm_type_gare_clv ;;
  }

  dimension: rate_designation {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_designation ;;
  }
  # Dates and timestamps can be represented in Looker using a dimension group of type: time.
  # Looker converts dates and timestamps to the specified timeframes within the dimension group.

  dimension_group: rate_gdat {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rate_gdat ;;
  }

  dimension: rate_id {
    view_label: "Rate Columns"
    type: number
    sql: ${TABLE}.rate_id ;;
  }

  dimension_group: rate_next_gdat {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rate_next_gdat ;;
  }

  dimension: rate_prl {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_prl ;;
  }

  dimension: rate_type {
    view_label: "Rate Columns"
    type: string
    sql: ${TABLE}.rate_type ;;
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

  dimension_group: rate_validity_end {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rate_validity_end ;;
  }

  dimension_group: rate_vdat {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rate_vdat ;;
  }

  dimension: rental_days {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.rental_days ;;
  }

  dimension_group: rntl_accounting_date {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rntl_accounting_date ;;
  }

  dimension: rntl_correction_mvnr {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rntl_correction_mvnr ;;
  }

  dimension: rntl_discount {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.rntl_discount ;;
  }

  dimension_group: rntl_handover_datm {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rntl_handover_datm ;;
  }

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

  dimension: rntl_one_way_mvnr {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rntl_one_way_mvnr ;;
  }
  
  dimension: rntl_bev_mvnr {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rntl_bev_mvnr ;;
  }

  dimension: rntl_payment_type {
    view_label: "Mietvertrag Columns"
    type: string
    sql: ${TABLE}.rntl_payment_type ;;
  }

  dimension_group: rntl_return_datm {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rntl_return_datm ;;
  }

  dimension: rntl_revenue {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.rntl_revenue ;;
  }


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

  dimension: rsrv_cancelled_resn {
    view_label: "Reservation Columns"
    type: number
    sql: ${TABLE}.rsrv_cancelled_resn ;;
  }

  dimension_group: rsrv_date {
    view_label: "Date Columns"
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.rsrv_date ;;
  }

  dimension: rsrv_new_customer {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_new_customer ;;
  }

  dimension: rsrv_noshow_resn {
    view_label: "Reservation Columns"
    type: number
    sql: ${TABLE}.rsrv_noshow_resn ;;
  }

  dimension: rsrv_posl_country_code {
    view_label: "Reservation Columns"
    type: string
    sql: ${TABLE}.rsrv_posl_country_code ;;
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

  dimension: rsts_excitement_num {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rsts_excitement_num ;;
  }

  dimension: rsts_recommendation_num {
    view_label: "Mietvertrag Columns"
    type: number
    sql: ${TABLE}.rsts_recommendation_num ;;
  }

  dimension: time_and_mileage {
    view_label: "Other Columns"
    hidden: yes
    type: number
    sql: ${TABLE}.time_and_mileage ;;
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: total_improved_revenue {
    view_label: "Measures"
    type: sum
    sql: ${improved_revenue} ;;
    value_format: "€0.00"
  }

  measure: average_improved_revenue {
    view_label: "Measures"
    type: average
    sql: ${improved_revenue} ;;
    value_format: "€0.00"
  }

  measure: total_rntl_discount {
    view_label: "Measures"
    type: sum
    sql: ${rntl_discount} ;;
    value_format: "€0.00"
  }

  measure: average_rntl_discount {
    view_label: "Measures"
    type: average
    sql: ${rntl_discount} ;;
    value_format: "€0.00"
  }

  measure: total_rntl_revenue {
    view_label: "Measures"
    type: sum
    sql: ${rntl_revenue} ;;
    value_format: "€0.00"
  }

  measure: average_rntl_revenue {
    view_label: "Measures"
    type: average
    sql: ${rntl_revenue} ;;
    value_format: "€0.00"
  }

  measure: total_mvnr_count {
    view_label: "Measures"
    type: sum
    sql: ${mvnr_count} ;;
  }

  measure: total_mvnrs {
    view_label: "Measures"
    type: count_distinct
    sql: ${rntl_mvnr} ;;
  }

  measure: total_one_way_mvnrs {
    view_label: "Measures"
    type: count_distinct
    sql: ${rntl_one_way_mvnr} ;;
  }

  measure: ratio_one_way_mvnrs {
    view_label: "Measures"
    type: number
    sql: 1.0*${total_one_way_mvnrs}/${total_mvnrs} ;;
        }

  measure: total_correction_mvnrs {
      view_label: "Measures"
      type: count_distinct
      sql: ${rntl_correction_mvnr} ;;
  }

  measure: ratio_correction_mvnrs {
    view_label: "Measures"
    type: number
    sql: 1.0*${total_correction_mvnrs}/${total_mvnrs} ;;
        }

  measure: total_bev_mvnrs {
    view_label: "Measures"
    type: count_distinct
    sql: ${rntl_bev_mvnr} ;;
  }
  
  measure: ratio_bev_mvnrs {
    view_label: "Measures"
    type: number
    sql: 1.0*${total_bev_mvnrs}/${total_mvnrs}
  }
  

  measure: total_reservations {
      view_label: "Measures"
      type: count_distinct
      sql: ${rsrv_resn} ;;
  }

  measure: total_cancelled_resn {
    view_label: "Measures"
    type: count_distinct
    sql: ${rsrv_cancelled_resn} ;;
  }

  measure: ratio_cancelled_resn {
    view_label: "Measures"
    type: number
    sql: 1.0*${total_cancelled_resn}/${total_reservations} ;;
        }

  measure: total_noshow_resn {
    view_label: "Measures"
    type: count_distinct
    sql: ${rsrv_noshow_resn} ;;
  }

  measure: ratio_noshow_resn {
    view_label: "Measures"
    type: number
    sql: 1.0*${total_noshow_resn}/${total_reservations} ;;
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

  measure: total_time_and_mileage {
    view_label: "Measures"
    type: sum
    sql: ${time_and_mileage} ;;
    value_format: "€0.00"
  }

  measure: average_time_and_mileage {
    view_label: "Measures"
    type: average
    sql: ${time_and_mileage} ;;
    value_format: "€0.00"
  }

  measure: total_inc_tot{
    view_label: "Measures"
    type: sum
    sql: ${inc_tot} ;;
    value_format: "€0.00"
  }

  measure: average_inc_tot {
    view_label: "Measures"
    type: average
    sql: ${inc_tot} ;;
    value_format: "€0.00"
  }

  measure: total_other_charges{
    view_label: "Measures"
    type: sum
    sql: ${total_improved_revenue} - {total_time_and_mileage} - {total_inc_tot} ;;
    value_format: "€0.00"
  }

  measure: avg_inc_tot {
    view_label: "Measures"
    type: average
    sql: ${average_improved_revenue} - {average_time_and_mileage} - {average_inc_tot} ;;
    value_format: "€0.00"
  }

  measure: avg_other_charges{
    view_label: "Measures"
    type: average
    sql: ${total_improved_revenue} - {total_time_and_mileage} - {total_inc_tot} ;;
    value_format: "€0.00"
  }

  measure: total_domestic_revenue {
    view_label: "Measures"
    type: sum
    sql: ${domestic_revenue} ;;
    value_format: "€0.00"
  }

  measure: ratio_domestic_revenue {
    view_label: "Measures"
    type: number
    sql: 1.0*${domestic_revenue}/${total_improved_revenue} ;;
        }

  measure: total_non_domestic_revenue {
    view_label: "Measures"
    type: sum
    sql: ${non_domestic_revenue} ;;
    value_format: "€0.00"
  }

  measure: ratio_non_domestic_revenue {
    view_label: "Measures"
    type: number
    sql: 1.0*${non_domestic_revenue}/${total_improved_revenue} ;;
        }

  measure: total_app_reservation_revenue {
    view_label: "Measures"
    type: sum
    sql: ${app_reservation_revenue} ;;
    value_format: "€0.00"
  }

  measure: count_app_reservation_mvnr {
    view_label: "Measures"
    type: count_distinct
    sql: ${app_reservation_mvnr} ;;
  }

  measure: ratio_app_reservation_mvnr {
    view_label: "Measures"
    type: number
    sql: 1.0*${count_app_reservation_mvnr}/${total_mvnrs} ;;
  }

  measure: total_internet_reservation_revenue {
    view_label: "Measures"
    type: sum
    sql: ${internet_reservation_revenue} ;;
    value_format: "€0.00"
  }

  measure: count_internet_reservation_mvnr {
    view_label: "Measures"
    type: count_distinct
    sql: ${internet_reservation_mvnr} ;;
  }

  measure: ratio_internet_reservation_mvnr {
    view_label: "Measures"
    type: number
    sql: 1.0*${count_internet_reservation_mvnr}/${total_mvnrs} ;;
  }

  measure: average_CES {
    view_label: "Measures"
    type: average
    sql: ${rsts_excitement_num} ;;
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
      cstm_name,
      dto_name,
      dfi_name,
      age_name,
      dtt_name,
      dfr_name,
      cstm_account_manager_name,
      product_level3_name,
      chrg_name
    ]
  }

}
