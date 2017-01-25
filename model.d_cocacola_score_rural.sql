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
  DROP VIEW IF EXISTS model.d_cocacola_score_rural CASCADE ;
  CREATE OR REPLACE VIEW model.d_cocacola_score_rural AS SELECT 
                                                              r. dw_dt,
                                                              r.  period, 
                                                              r.  mbd, 
                                                              r.  bg, 
                                                              r.  bottler, 
                                                              r.  channel,
                                                              r.  code, 
                                                              null::TEXT as item ,
                                                              r.  fact,
                                                              sum(r.value::NUMERIC(30,16)) as value,
                                                              m.abbrevation
  FROM  ods.d_cocacola_score_rural r
  INNER JOIN conf.d_cocacola_bottlers_mapping m
  ON r.bottler = m.bottlers_rural_score
  WHERE bottler NOT IN ('Tier1','Tier2','Tier3','Sichuan Area','Tianjin Area','Liaoning') group by r.dw_dt,r.period,r.mbd,r.bg,r.bottler,r.channel,r.code,r.fact,m.abbrevation;
