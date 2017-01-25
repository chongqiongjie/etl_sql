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
  CREATE TABLE IF NOT EXISTS stg.d_cocacola_product_rng (
    dw_start_dt CHAR(10),
    dw_end_dt CHAR(10),
    dw_in_use CHAR(1),
    dw_ld_cnt INT,
    dw_ld_ts CHAR(22)
  ) ;

  CREATE TABLE IF NOT EXISTS ods.d_cocacola_product (
    dw_dt CHAR(10),
    market TEXT, 
    bg TEXT, 
    bottler TEXT, 
    channel TEXT, 
    product TEXT, 
    category TEXT, 
    manu TEXT, 
    brand TEXT, 
    period TEXT, 
    total_availability TEXT, 
    shelf_availability TEXT,
    cooler_availability TEXT, 
    total_facing_number TEXT, 
    shelf_facing_number TEXT, 
    cooler_facing_number TEXT, 
    total_sovi TEXT, 
    shelf_sovi TEXT, 
    cooler_sovi TEXT, 
    average_price TEXT, 
    price_communication_avai TEXT, 
    margin TEXT
  );
  
  update stg.d_cocacola_product_rng set dw_in_use = '0' where dw_in_use = '1' ;
  insert into stg.d_cocacola_product_rng (
    dw_start_dt,
    dw_end_dt,
    dw_in_use,
    dw_ld_cnt,
    dw_ld_ts
  ) select
    TO_CHAR(TO_DATE(MIN(period), 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_start_dt,
    TO_CHAR(TO_DATE(MAX(period), 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_end_dt,
    '1' AS dw_in_use,
    COUNT(1) AS dw_ld_cnt,
    TO_CHAR(CURRENT_TIMESTAMP(0), 'YYYY-MM-DD"T"HH24:MI:SSOF') AS dw_ld_ts
  from stg.d_cocacola_product ;

  delete from ods.d_cocacola_product ods
  using stg.d_cocacola_product_rng rng
  where rng.dw_in_use = '1'
  and ods.dw_dt between rng.dw_start_dt and rng.dw_end_dt ;

  insert into ods.d_cocacola_product (
    dw_dt,
    market,
    bg,
    bottler,
    channel,
    product,
    category,
    manu,
    brand,
    period,
    total_availability,
    shelf_availability,
    cooler_availability,
    total_facing_number,
    shelf_facing_number,
    cooler_facing_number,
    total_sovi,
    shelf_sovi,
    cooler_sovi,
    average_price,
    price_communication_avai,
    margin 
  ) select
    TO_CHAR(TO_DATE(period, 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_dt,
    market,
    bg,
    bottler,
    channel,
    product,
    category,
    manu,
    brand,
    period,
    total_availability,
    shelf_availability,
    cooler_availability,
    total_facing_number,
    shelf_facing_number,
    cooler_facing_number,
    total_sovi,
    shelf_sovi,
    cooler_sovi,
    average_price,
    price_communication_avai,
    margin
  from stg.d_cocacola_product ;
