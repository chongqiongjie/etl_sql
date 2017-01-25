 --**********************************
 --* [intro]
 --*   author=larluo@spiderdt.com
 --*   func=partition algorithm for data warehouse
 --*=================================
 --* [param]
 --*   tabname=staging table name
 --*   prt_cols_str=ods partition cols
 --*=================================
 --* [caller]
 --*   spark_etl_prt.groovy d_bolome_dau '[[":p_date", 1]]'
 --*   spark_etl_prt.groovy d_bolome_events
 --*   spark_etl_prt.groovy d_bolome_inventory
 --*   spark_etl_prt.groovy d_bolome_orders
 --*=================================
 --* [version]
 --*   v1_0=2016-09-28@larluo{create}
 --**********************************

  \c dw ;
  CREATE TABLE IF NOT EXISTS stg.d_cocacola_product (
    MARKET text, 
    BG text, 
    BOTTLER text, 
    CHANNEL text, 
    PRODUCT text, 
    CATEGORY text, 
    MANU text, 
    BRAND text, 
    PERIOD text, 
    TOTAL_AVAILABILITY text, 
    SHELF_AVAILABILITY text,
    COOLER_AVAILABILITY text, 
    TOTAL_FACING_NUMBER text, 
    SHELF_FACING_NUMBER text, 
    COOLER_FACING_NUMBER text, 
    TOTAL_SOVI text, 
    SHELF_SOVI text, 
    COOLER_SOVI text, 
    AVERAGE_PRICE text, 
    PRICE_COMMUNICATION_AVAI text, 
    MARGIN text
    );
  truncate table stg.d_cocacola_product;
  \copy stg.d_cocacola_product FROM '/home/spiderdt/work/git/spiderdt-team/var/data/stg.db/d_cocacola_product/__DATA__.csv' CSV HEADER ;
