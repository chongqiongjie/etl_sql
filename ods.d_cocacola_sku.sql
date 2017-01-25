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
  CREATE TABLE IF NOT EXISTS stg.d_cocacola_sku_rng (
    dw_start_dt CHAR(10),
    dw_end_dt CHAR(10),
    dw_in_use CHAR(1),
    dw_ld_cnt INT,
    dw_ld_ts CHAR(22)
  ) ;
  CREATE TABLE IF NOT EXISTS ods.d_cocacola_sku (
    dw_dt CHAR(10), 
    period TEXT,
    mbd TEXT,
    bottler_group TEXT,
    bottler TEXT,
    channel TEXT,
    product0 TEXT,
    product1 TEXT,
    product2 TEXT,
    product3 TEXT,
    product4 TEXT,
    product5 TEXT,
    product6 TEXT
  ) ;

  UPDATE stg.d_cocacola_sku_rng SET dw_in_use = '0' WHERE dw_in_use = '1' ;
  INSERT INTO stg.d_cocacola_sku_rng (
    dw_start_dt,
    dw_end_dt,
    dw_in_use,
    dw_ld_cnt,
    dw_ld_ts
  ) SELECT
    TO_CHAR(TO_DATE(MIN(period), 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_start_dt,
    TO_CHAR(TO_DATE(MAX(period), 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_end_dt,
    '1' AS dw_in_use,
    COUNT(1) AS dw_ld_cnt,
    TO_CHAR(CURRENT_TIMESTAMP(0), 'YYYY-MM-DD"T"HH24:MI:SSOF') AS dw_ld_ts
  FROM stg.d_cocacola_sku ;

  DELETE FROM ods.d_cocacola_sku ods
  USING stg.d_cocacola_sku_rng rng
  WHERE rng.dw_in_use = '1'
  AND ods.dw_dt BETWEEN rng.dw_start_dt AND rng.dw_end_dt ;

  INSERT INTO ods.d_cocacola_sku (
    dw_dt,
    period,
    mbd,
    bottler_group,
    bottler,
    channel,
    product0,
    product1,
    product2,
    product3,
    product4,
    product5,
    product6
  ) SELECT
    TO_CHAR(TO_DATE(period, 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_dt,
    period,
    mbd,
    bottler_group,
    bottler,
    channel,
    product0,
    product1,
    product2,
    product3,
    product4,
    product5,
    product6
  FROM stg.d_cocacola_sku ;
