// Databricks notebook source
// MAGIC %sql
// MAGIC show databases

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE sqlendpoint1000d

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC STORAGE_ACCOUNT = "scbpocdev"
// MAGIC CONTAINER = "dbstorage1000d"
// MAGIC MOUNT_POINT = "/mnt/azureblobnew1000d"
// MAGIC SAS_KEY = "sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-10-20T00:02:07Z&st=2021-10-19T16:02:07Z&spr=https&sig=WYVSvQFR8otMZtaxkiQoqxpYQ6xDWeUrOgj62G6U7po%3D" 
// MAGIC #Grant all permissions via Shared Access Signature
// MAGIC 
// MAGIC source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
// MAGIC conf_key = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
// MAGIC 
// MAGIC try:
// MAGIC   dbutils.fs.mount(
// MAGIC     source = source_str,
// MAGIC     mount_point = MOUNT_POINT,
// MAGIC     extra_configs = {conf_key: SAS_KEY}
// MAGIC   )
// MAGIC except Exception as e:
// MAGIC   print("ERROR: {} already mounted.".format(MOUNT_POINT))

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta")

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC drop table sqlendpoint1000d.call_center;
// MAGIC drop table sqlendpoint1000d.catalog_returns;
// MAGIC drop table sqlendpoint1000d.customer;
// MAGIC drop table sqlendpoint1000d.customer_demographics;
// MAGIC drop table sqlendpoint1000d.household_demographics;
// MAGIC drop table sqlendpoint1000d.inventory;
// MAGIC drop table sqlendpoint1000d.promotion;
// MAGIC drop table sqlendpoint1000d.ship_mode;
// MAGIC drop table sqlendpoint1000d.store_sales;
// MAGIC drop table sqlendpoint1000d.time_dim;
// MAGIC drop table sqlendpoint1000d.web_page;
// MAGIC drop table sqlendpoint1000d.web_sales;
// MAGIC drop table sqlendpoint1000d.web_site;
// MAGIC drop table sqlendpoint1000d.catalog_page;
// MAGIC drop table sqlendpoint1000d.catalog_sales;
// MAGIC drop table sqlendpoint1000d.customer_address;
// MAGIC drop table sqlendpoint1000d.date_dim;
// MAGIC drop table sqlendpoint1000d.income_band;
// MAGIC drop table sqlendpoint1000d.item;
// MAGIC drop table sqlendpoint1000d.reason;
// MAGIC drop table sqlendpoint1000d.store;
// MAGIC drop table sqlendpoint1000d.store_returns;
// MAGIC drop table sqlendpoint1000d.warehouse;
// MAGIC drop table sqlendpoint1000d.web_returns;

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE `sqlendpoint1000d`.`call_center` (`cc_call_center_sk` INT, `cc_call_center_id` STRING, `cc_rec_start_date` DATE, `cc_rec_end_date` DATE, `cc_closed_date_sk` INT, `cc_open_date_sk` INT, `cc_name` STRING, `cc_class` STRING, `cc_employees` INT, `cc_sq_ft` INT, `cc_hours` STRING, `cc_manager` STRING, `cc_mkt_id` INT, `cc_mkt_class` STRING, `cc_mkt_desc` STRING, `cc_market_manager` STRING, `cc_division` INT, `cc_division_name` STRING, `cc_company` INT, `cc_company_name` STRING, `cc_street_number` STRING, `cc_street_name` STRING, `cc_street_type` STRING, `cc_suite_number` STRING, `cc_city` STRING, `cc_county` STRING, `cc_state` STRING, `cc_zip` STRING, `cc_country` STRING, `cc_gmt_offset` DECIMAL(5,2), `cc_tax_percentage` DECIMAL(5,2))         
// MAGIC USING delta
// MAGIC OPTIONS (                         
// MAGIC   
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/call_center'
// MAGIC );                                                                                    
// MAGIC 
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`catalog_page` (`cp_catalog_page_sk` INT, `cp_catalog_page_id` STRING, `cp_start_date_sk` INT, `cp_end_date_sk` INT, `cp_department` STRING, `cp_catalog_number` INT, `cp_catalog_page_number` INT, `cp_description` STRING, `cp_type` STRING)                                 
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/catalog_page'                                                                                                             
// MAGIC );       
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`catalog_returns` (`cr_returned_time_sk` INT, `cr_item_sk` INT, `cr_refunded_customer_sk` INT, `cr_refunded_cdemo_sk` INT, `cr_refunded_hdemo_sk` INT, `cr_refunded_addr_sk` INT, `cr_returning_customer_sk` INT, `cr_returning_cdemo_sk` INT, `cr_returning_hdemo_sk` INT, `cr_returning_addr_sk` INT, `cr_call_center_sk` INT, `cr_catalog_page_sk` INT, `cr_ship_mode_sk` INT, `cr_warehouse_sk` INT, `cr_reason_sk` INT, `cr_order_number` BIGINT, `cr_return_quantity` INT, `cr_return_amount` DECIMAL(7,2), `cr_return_tax` DECIMAL(7,2), `cr_return_amt_inc_tax` DECIMAL(7,2), `cr_fee` DECIMAL(7,2), `cr_return_ship_cost` DECIMAL(7,2), `cr_refunded_cash` DECIMAL(7,2), `cr_reversed_charge` DECIMAL(7,2), `cr_store_credit` DECIMAL(7,2), `cr_net_loss` DECIMAL(7,2), `cr_returned_date_sk` INT)                                                            
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/catalog_returns'                                                                                                          
// MAGIC )                               
// MAGIC PARTITIONED BY (cr_returned_date_sk);
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`catalog_sales` (`cs_sold_time_sk` INT, `cs_ship_date_sk` INT, `cs_bill_customer_sk` INT, `cs_bill_cdemo_sk` INT, `cs_bill_hdemo_sk` INT, `cs_bill_addr_sk` INT, `cs_ship_customer_sk` INT, `cs_ship_cdemo_sk` INT, `cs_ship_hdemo_sk` INT, `cs_ship_addr_sk` INT, `cs_call_center_sk` INT, `cs_catalog_page_sk` INT, `cs_ship_mode_sk` INT, `cs_warehouse_sk` INT, `cs_item_sk` INT, `cs_promo_sk` INT, `cs_order_number` BIGINT, `cs_quantity` INT, `cs_wholesale_cost` DECIMAL(7,2), `cs_list_price` DECIMAL(7,2), `cs_sales_price` DECIMAL(7,2), `cs_ext_discount_amt` DECIMAL(7,2), `cs_ext_sales_price` DECIMAL(7,2), `cs_ext_wholesale_cost` DECIMAL(7,2), `cs_ext_list_price` DECIMAL(7,2), `cs_ext_tax` DECIMAL(7,2), `cs_coupon_amt` DECIMAL(7,2), `cs_ext_ship_cost` DECIMAL(7,2), `cs_net_paid` DECIMAL(7,2), `cs_net_paid_inc_tax` DECIMAL(7,2), `cs_net_paid_inc_ship` DECIMAL(7,2), `cs_net_paid_inc_ship_tax` DECIMAL(7,2), `cs_net_profit` DECIMAL(7,2), `cs_sold_date_sk` INT)                     
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/catalog_sales'                                                                                                            
// MAGIC )                               
// MAGIC PARTITIONED BY (cs_sold_date_sk);                                                                                                            
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`customer` (`c_customer_sk` INT, `c_customer_id` STRING, `c_current_cdemo_sk` INT, `c_current_hdemo_sk` INT, `c_current_addr_sk` INT, `c_first_shipto_date_sk` INT, `c_first_sales_date_sk` INT, `c_salutation` STRING, `c_first_name` STRING, `c_last_name` STRING, `c_preferred_cust_flag` STRING, `c_birth_day` INT, `c_birth_month` INT, `c_birth_year` INT, `c_birth_country` STRING, `c_login` STRING, `c_email_address` STRING, `c_last_review_date` STRING)                                                                                                        
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/customer'   
// MAGIC );           
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`customer_address` (`ca_address_sk` INT, `ca_address_id` STRING, `ca_street_number` STRING, `ca_street_name` STRING, `ca_street_type` STRING, `ca_suite_number` STRING, `ca_city` STRING, `ca_county` STRING, `ca_state` STRING, `ca_zip` STRING, `ca_country` STRING, `ca_gmt_offset` DECIMAL(5,2), `ca_location_type` STRING)                                                                                              
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/customer_address'                                                                                                         
// MAGIC );                                       
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`customer_demographics` (`cd_demo_sk` INT, `cd_gender` STRING, `cd_marital_status` STRING, `cd_education_status` STRING, `cd_purchase_estimate` INT, `cd_credit_rating` STRING, `cd_dep_count` INT, `cd_dep_employed_count` INT, `cd_dep_college_count` INT)                   USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/customer_demographics'                                                                                                    
// MAGIC );                               
// MAGIC                                   
// MAGIC CREATE TABLE `sqlendpoint1000d`.`date_dim` (`d_date_sk` INT, `d_date_id` STRING, `d_date` DATE, `d_month_seq` INT, `d_week_seq` INT, `d_quarter_seq` INT, `d_year` INT, `d_dow` INT, `d_moy` INT, `d_dom` INT, `d_qoy` INT, `d_fy_year` INT, `d_fy_quarter_seq` INT, `d_fy_week_seq` INT, `d_day_name` STRING, `d_quarter_name` STRING, `d_holiday` STRING, `d_weekend` STRING, `d_following_holiday` STRING, `d_first_dom` INT, `d_last_dom` INT, `d_same_day_ly` INT, `d_same_day_lq` INT, `d_current_day` STRING, `d_current_week` STRING, `d_current_month` STRING, `d_current_quarter` STRING, `d_current_year` STRING)               
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/date_dim'   
// MAGIC );                                   
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`household_demographics` (`hd_demo_sk` INT, `hd_income_band_sk` INT, `hd_buy_potential` STRING, `hd_dep_count` INT, `hd_vehicle_count` INT)                      
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/household_demographics'                                                                                                   
// MAGIC );                                                   
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`income_band` (`ib_income_band_sk` INT, `ib_lower_bound` INT, `ib_upper_bound` INT)                                              
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/income_band'
// MAGIC );             
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`inventory` (`inv_item_sk` INT, `inv_warehouse_sk` INT, `inv_quantity_on_hand` INT, `inv_date_sk` INT)                           
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/inventory'  
// MAGIC )                               
// MAGIC PARTITIONED BY (inv_date_sk);
// MAGIC 
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`item` (`i_item_sk` INT, `i_item_id` STRING, `i_rec_start_date` DATE, `i_rec_end_date` DATE, `i_item_desc` STRING, `i_current_price` DECIMAL(7,2), `i_wholesale_cost` DECIMAL(7,2), `i_brand_id` INT, `i_brand` STRING, `i_class_id` INT, `i_class` STRING, `i_category_id` INT, `i_category` STRING, `i_manufact_id` INT, `i_manufact` STRING, `i_size` STRING, `i_formulation` STRING, `i_color` STRING, `i_units` STRING, `i_container` STRING, `i_manager_id` INT, `i_product_name` STRING)                                                                            
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/item'       
// MAGIC ) ;                    
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`promotion` (`p_promo_sk` INT, `p_promo_id` STRING, `p_start_date_sk` INT, `p_end_date_sk` INT, `p_item_sk` INT, `p_cost` DECIMAL(15,2), `p_response_target` INT, `p_promo_name` STRING, `p_channel_dmail` STRING, `p_channel_email` STRING, `p_channel_catalog` STRING, `p_channel_tv` STRING, `p_channel_radio` STRING, `p_channel_press` STRING, `p_channel_event` STRING, `p_channel_demo` STRING, `p_channel_details` STRING, `p_purpose` STRING, `p_discount_active` STRING)                                                                                         
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/promotion'  
// MAGIC ) ;                    
// MAGIC 
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`reason` (`r_reason_sk` INT, `r_reason_id` STRING, `r_reason_desc` STRING)                                                       
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/reason'     
// MAGIC ) ;                              
// MAGIC          
// MAGIC CREATE TABLE `sqlendpoint1000d`.`ship_mode` (`sm_ship_mode_sk` INT, `sm_ship_mode_id` STRING, `sm_type` STRING, `sm_code` STRING, `sm_carrier` STRING, `sm_contract` STRING)                     
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/ship_mode'  
// MAGIC ) ;           
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`store` (`s_store_sk` INT, `s_store_id` STRING, `s_rec_start_date` DATE, `s_rec_end_date` DATE, `s_closed_date_sk` INT, `s_store_name` STRING, `s_number_employees` INT, `s_floor_space` INT, `s_hours` STRING, `s_manager` STRING, `s_market_id` INT, `s_geography_class` STRING, `s_market_desc` STRING, `s_market_manager` STRING, `s_division_id` INT, `s_division_name` STRING, `s_company_id` INT, `s_company_name` STRING, `s_street_number` STRING, `s_street_name` STRING, `s_street_type` STRING, `s_suite_number` STRING, `s_city` STRING, `s_county` STRING, `s_state` STRING, `s_zip` STRING, `s_country` STRING, `s_gmt_offset` DECIMAL(5,2), `s_tax_precentage` DECIMAL(5,2))                             
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/store'      
// MAGIC )  ;                     
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`store_returns` (`sr_return_time_sk` INT, `sr_item_sk` INT, `sr_customer_sk` INT, `sr_cdemo_sk` INT, `sr_hdemo_sk` INT, `sr_addr_sk` INT, `sr_store_sk` INT, `sr_reason_sk` INT, `sr_ticket_number` BIGINT, `sr_return_quantity` INT, `sr_return_amt` DECIMAL(7,2), `sr_return_tax` DECIMAL(7,2), `sr_return_amt_inc_tax` DECIMAL(7,2), `sr_fee` DECIMAL(7,2), `sr_return_ship_cost` DECIMAL(7,2), `sr_refunded_cash` DECIMAL(7,2), `sr_reversed_charge` DECIMAL(7,2), `sr_store_credit` DECIMAL(7,2), `sr_net_loss` DECIMAL(7,2), `sr_returned_date_sk` INT)              
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/store_returns'                                                                                                            
// MAGIC )                               
// MAGIC PARTITIONED BY (sr_returned_date_sk)  ; 
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`store_sales` (`ss_sold_time_sk` INT, `ss_item_sk` INT, `ss_customer_sk` INT, `ss_cdemo_sk` INT, `ss_hdemo_sk` INT, `ss_addr_sk` INT, `ss_store_sk` INT, `ss_promo_sk` INT, `ss_ticket_number` BIGINT, `ss_quantity` INT, `ss_wholesale_cost` DECIMAL(7,2), `ss_list_price` DECIMAL(7,2), `ss_sales_price` DECIMAL(7,2), `ss_ext_discount_amt` DECIMAL(7,2), `ss_ext_sales_price` DECIMAL(7,2), `ss_ext_wholesale_cost` DECIMAL(7,2), `ss_ext_list_price` DECIMAL(7,2), `ss_ext_tax` DECIMAL(7,2), `ss_coupon_amt` DECIMAL(7,2), `ss_net_paid` DECIMAL(7,2), `ss_net_paid_inc_tax` DECIMAL(7,2), `ss_net_profit` DECIMAL(7,2), `ss_sold_date_sk` INT)                                                                    
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/store_sales'
// MAGIC )                               
// MAGIC PARTITIONED BY (ss_sold_date_sk)   ;
// MAGIC 
// MAGIC 
// MAGIC      
// MAGIC CREATE TABLE `sqlendpoint1000d`.`time_dim` (`t_time_sk` INT, `t_time_id` STRING, `t_time` INT, `t_hour` INT, `t_minute` INT, `t_second` INT, `t_am_pm` STRING, `t_shift` STRING, `t_sub_shift` STRING, `t_meal_time` STRING)                                                                                   
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/time_dim'   
// MAGIC );
// MAGIC 
// MAGIC                                
// MAGIC CREATE TABLE `sqlendpoint1000d`.`warehouse` (`w_warehouse_sk` INT, `w_warehouse_id` STRING, `w_warehouse_name` STRING, `w_warehouse_sq_ft` INT, `w_street_number` STRING, `w_street_name` STRING, `w_street_type` STRING, `w_suite_number` STRING, `w_city` STRING, `w_county` STRING, `w_state` STRING, `w_zip` STRING, `w_country` STRING, `w_gmt_offset` DECIMAL(5,2))                                                                                    
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/warehouse'  
// MAGIC ) ;                              
// MAGIC                 
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`web_page` (`wp_web_page_sk` INT, `wp_web_page_id` STRING, `wp_rec_start_date` DATE, `wp_rec_end_date` DATE, `wp_creation_date_sk` INT, `wp_access_date_sk` INT, `wp_autogen_flag` STRING, `wp_customer_sk` INT, `wp_url` STRING, `wp_type` STRING, `wp_char_count` INT, `wp_link_count` INT, `wp_image_count` INT, `wp_max_ad_count` INT)                                                                                   USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/web_page'   
// MAGIC )  ;
// MAGIC 
// MAGIC          
// MAGIC CREATE TABLE `sqlendpoint1000d`.`web_returns` (`wr_returned_time_sk` INT, `wr_item_sk` INT, `wr_refunded_customer_sk` INT, `wr_refunded_cdemo_sk` INT, `wr_refunded_hdemo_sk` INT, `wr_refunded_addr_sk` INT, `wr_returning_customer_sk` INT, `wr_returning_cdemo_sk` INT, `wr_returning_hdemo_sk` INT, `wr_returning_addr_sk` INT, `wr_web_page_sk` INT, `wr_reason_sk` INT, `wr_order_number` BIGINT, `wr_return_quantity` INT, `wr_return_amt` DECIMAL(7,2), `wr_return_tax` DECIMAL(7,2), `wr_return_amt_inc_tax` DECIMAL(7,2), `wr_fee` DECIMAL(7,2), `wr_return_ship_cost` DECIMAL(7,2), `wr_refunded_cash` DECIMAL(7,2), `wr_reversed_charge` DECIMAL(7,2), `wr_account_credit` DECIMAL(7,2), `wr_net_loss` DECIMAL(7,2), `wr_returned_date_sk` INT)                              
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/web_returns'
// MAGIC )                               
// MAGIC PARTITIONED BY (wr_returned_date_sk) ;
// MAGIC 
// MAGIC                                                                                                   
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`web_sales` (`ws_sold_time_sk` INT, `ws_ship_date_sk` INT, `ws_item_sk` INT, `ws_bill_customer_sk` INT, `ws_bill_cdemo_sk` INT, `ws_bill_hdemo_sk` INT, `ws_bill_addr_sk` INT, `ws_ship_customer_sk` INT, `ws_ship_cdemo_sk` INT, `ws_ship_hdemo_sk` INT, `ws_ship_addr_sk` INT, `ws_web_page_sk` INT, `ws_web_site_sk` INT, `ws_ship_mode_sk` INT, `ws_warehouse_sk` INT, `ws_promo_sk` INT, `ws_order_number` BIGINT, `ws_quantity` INT, `ws_wholesale_cost` DECIMAL(7,2), `ws_list_price` DECIMAL(7,2), `ws_sales_price` DECIMAL(7,2), `ws_ext_discount_amt` DECIMAL(7,2), `ws_ext_sales_price` DECIMAL(7,2), `ws_ext_wholesale_cost` DECIMAL(7,2), `ws_ext_list_price` DECIMAL(7,2), `ws_ext_tax` DECIMAL(7,2), `ws_coupon_amt` DECIMAL(7,2), `ws_ext_ship_cost` DECIMAL(7,2), `ws_net_paid` DECIMAL(7,2), `ws_net_paid_inc_tax` DECIMAL(7,2), `ws_net_paid_inc_ship` DECIMAL(7,2), `ws_net_paid_inc_ship_tax` DECIMAL(7,2), `ws_net_profit` DECIMAL(7,2), `ws_sold_date_sk` INT)                                
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/web_sales'  
// MAGIC )                               
// MAGIC PARTITIONED BY (ws_sold_date_sk) ;
// MAGIC 
// MAGIC 
// MAGIC CREATE TABLE `sqlendpoint1000d`.`web_site` (`web_site_sk` INT, `web_site_id` STRING, `web_rec_start_date` DATE, `web_rec_end_date` DATE, `web_name` STRING, `web_open_date_sk` INT, `web_close_date_sk` INT, `web_class` STRING, `web_manager` STRING, `web_mkt_id` INT, `web_mkt_class` STRING, `web_mkt_desc` STRING, `web_market_manager` STRING, `web_company_id` INT, `web_company_name` STRING, `web_street_number` STRING, `web_street_name` STRING, `web_street_type` STRING, `web_suite_number` STRING, `web_city` STRING, `web_county` STRING, `web_state` STRING, `web_zip` STRING, `web_country` STRING, `web_gmt_offset` DECIMAL(5,2), `web_tax_percentage` DECIMAL(5,2))                                                                   
// MAGIC USING delta
// MAGIC OPTIONS (                       
// MAGIC 
// MAGIC   path '/mnt/azureblobnew1000d/TPC-DS/SourceFiles_delta/web_site'   
// MAGIC ) ;                              

// COMMAND ----------

// MAGIC %sql
// MAGIC use sqlendpoint1000d;
// MAGIC show tables

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE sqlendpoint1000d.catalog_returns RECOVER PARTITIONS;
// MAGIC ALTER TABLE sqlendpoint1000d.catalog_sales RECOVER PARTITIONS;
// MAGIC ALTER TABLE sqlendpoint1000d.inventory RECOVER PARTITIONS;
// MAGIC ALTER TABLE sqlendpoint1000d.store_returns RECOVER PARTITIONS;
// MAGIC ALTER TABLE sqlendpoint1000d.store_sales RECOVER PARTITIONS;
// MAGIC ALTER TABLE sqlendpoint1000d.web_returns RECOVER PARTITIONS;
// MAGIC ALTER TABLE sqlendpoint1000d.web_sales RECOVER PARTITIONS;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from sqlendpoint1000d.catalog_returns;
// MAGIC select count(*) from sqlendpoint1000d.catalog_sales;
// MAGIC select count(*) from sqlendpoint1000d.inventory;
// MAGIC select count(*) from sqlendpoint1000d.store_returns;
// MAGIC select count(*) from sqlendpoint1000d.store_sales;
// MAGIC select count(*) from sqlendpoint1000d.web_returns;
// MAGIC select count(*) from sqlendpoint1000d.web_sales;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*), 'call_center' from sqlendpoint1000d.call_center union
// MAGIC select count(*), 'catalog_page' from sqlendpoint1000d.catalog_page union
// MAGIC select count(*), 'catalog_returns' from sqlendpoint1000d.catalog_returns union
// MAGIC select count(*), 'catalog_sales' from sqlendpoint1000d.catalog_sales union
// MAGIC select count(*), 'customer' from sqlendpoint1000d.customer union
// MAGIC select count(*), 'customer_address' from sqlendpoint1000d.customer_address union
// MAGIC select count(*), 'customer_demographics' from sqlendpoint1000d.customer_demographics union
// MAGIC select count(*), 'date_dim' from sqlendpoint1000d.date_dim union
// MAGIC select count(*), 'household_demographics' from sqlendpoint1000d.household_demographics union
// MAGIC select count(*), 'income_band' from sqlendpoint1000d.income_band union
// MAGIC select count(*), 'inventory' from sqlendpoint1000d.inventory union
// MAGIC select count(*), 'item' from sqlendpoint1000d.item union
// MAGIC select count(*), 'promotion' from sqlendpoint1000d.promotion union
// MAGIC select count(*), 'reason' from sqlendpoint1000d.reason union
// MAGIC select count(*), 'ship_mode' from sqlendpoint1000d.ship_mode union
// MAGIC select count(*), 'store' from sqlendpoint1000d.store union
// MAGIC select count(*), 'store_returns' from sqlendpoint1000d.store_returns union
// MAGIC select count(*), 'store_sales' from sqlendpoint1000d.store_sales union
// MAGIC select count(*), 'time_dim' from sqlendpoint1000d.time_dim union
// MAGIC select count(*), 'warehouse' from sqlendpoint1000d.warehouse union
// MAGIC select count(*), 'web_page' from sqlendpoint1000d.web_page union
// MAGIC select count(*), 'web_returns' from sqlendpoint1000d.web_returns union
// MAGIC select count(*), 'web_sales' from sqlendpoint1000d.web_sales union
// MAGIC select count(*), 'web_site' from sqlendpoint1000d.web_site

// COMMAND ----------

// MAGIC %scala
// MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
// MAGIC 
// MAGIC 
// MAGIC val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// MAGIC 
// MAGIC val databaseName = "sqlendpoint1000d" // name of database with TPCDS data.
// MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
// MAGIC val format = "delta" // valid spark format like parquet "parquet".
// MAGIC 
// MAGIC val tables = new TPCDSTables(sqlContext,
// MAGIC     dsdgenDir = "/tmp/tpcds-kit/tools", // location of dsdgen
// MAGIC     scaleFactor = scaleFactor,
// MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
// MAGIC     useStringForDate = false) // true to replace DateType with StringType
// MAGIC  
// MAGIC // Set:
// MAGIC sql(s"use $databaseName")
// MAGIC  
// MAGIC // For CBO only, gather statistics on all columns:
// MAGIC tables.analyzeTables(databaseName, analyzeColumns = true)  

// COMMAND ----------

// MAGIC %scala
// MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDS
// MAGIC  
// MAGIC // Note: Declare "sqlContext" for Spark 2.x version
// MAGIC val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// MAGIC  
// MAGIC val tpcds = new TPCDS (sqlContext = sqlContext)
// MAGIC // Set:
// MAGIC val databaseName = "sqlendpoint1000d" // name of database with TPCDS data.
// MAGIC sql(s"use $databaseName")
// MAGIC 
// MAGIC val resultLocation = "wasbs://dbstorage1000d@scbpocdev.blob.core.windows.net/rd2_1_tb_adb_spark_tpcds_results" // place to write results
// MAGIC val iterations = 3 // how many iterations of queries to run.
// MAGIC val queries = tpcds.tpcds2_4Queries // queries to run.
// MAGIC val timeout = 24*60*60 // timeout, in seconds.
// MAGIC // Run:
// MAGIC val experiment = tpcds.runExperiment(
// MAGIC   queries, 
// MAGIC   iterations = iterations,
// MAGIC   resultLocation = resultLocation,
// MAGIC   forkThread = true)
// MAGIC experiment.waitForFinish(timeout)

// COMMAND ----------



// COMMAND ----------


import com.databricks.spark.sql.perf.tpcds.TPCDS
 
// Note: Declare "sqlContext" for Spark 2.x version
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
val tpcds = new TPCDS (sqlContext = sqlContext)
// Set:
val databaseName = "sqlendpoint1000d" // name of database with TPCDS data.
sql(s"use $databaseName")

val resultLocation = "wasbs://dbstorage1000d@scbpocdev.blob.core.windows.net/rd2_1_tb_adb_spark_tpcds_results" // place to write results
val iterations = 3 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24*60*60 // timeout, in seconds.
// Run:
val experiment = tpcds.runExperiment(
  queries, 
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)

// COMMAND ----------

import org.apache.spark.sql.functions._

val resultLocation = "/mnt/azureblobnew1000d/TPC-DS/tpcds_results/" // place to write results
val result = spark.read.json(resultLocation).filter("timestamp = 1636030088618").select(explode($"results").as("r"))
result.createOrReplaceTempView("result") 
spark.sql("select substring(r.name,1,100) as Name, bround((r.parsingTime+r.analysisTime+r.optimizationTime+r.planningTime+r.executionTime)/1000.0,1) as Runtime_sec  from result").show(3000)
