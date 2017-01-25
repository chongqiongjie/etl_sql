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
  CREATE TABLE IF NOT EXISTS stg.d_cocacola_score_rng (
    dw_start_dt CHAR(10),
    dw_end_dt CHAR(10),
    dw_in_use CHAR(1),
    dw_ld_cnt INT,
    dw_ld_ts CHAR(22)
  ) ;

  CREATE TABLE IF NOT EXISTS ods.d_cocacola_score_scd (
    dw_key TEXT,
    dw_var TEXT,
    dw_dts CHAR(10)[],
    dw_val_kv TEXT
  ) ;
  CREATE TABLE IF NOT EXISTS ods.d_cocacola_score (
    dw_dt CHAR(10),
    period TEXT,
    mbd TEXT,
    bg TEXT,
    bottler TEXT,
    channel TEXT,
    code TEXT,
    item TEXT,
    fact TEXT,
    value TEXT
  );
  
  update stg.d_cocacola_score_rng set dw_in_use = '0' where dw_in_use = '1' ;
  insert into stg.d_cocacola_score_rng (
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
  from stg.d_cocacola_score ;

  delete from ods.d_cocacola_score ods
  using stg.d_cocacola_score_rng rng
  where rng.dw_in_use = '1'
  and ods.dw_dt between rng.dw_start_dt and rng.dw_end_dt ;

  insert into ods.d_cocacola_score (
    dw_dt,
    period,
    mbd,
    bg,
    bottler,
    channel,
    code,
    item,
    fact,
    value
  ) select
    TO_CHAR(TO_DATE(period, 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_dt,
    period,
    mbd,
    bg, 
    bottler,
    channel,
    code,
    item,
    fact,
    value
  from stg.d_cocacola_score ;
