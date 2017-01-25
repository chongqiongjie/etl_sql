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
  \c dw;
  DROP VIEW  IF EXISTS model.d_cocacola_score CASCADE;
  CREATE OR REPLACE VIEW model.d_cocacola_score AS 
  SELECT s.dw_dt, s.period,trim(s.mbd) as mbd, s.bg, s.bottler, s.channel,s.code, (CASE WHEN s.code IS NULL THEN CONCAT(s.item, '-',s.channel) ELSE item END) AS item,s.fact,s.value::NUMERIC(30,16),m.abbrevation 
  FROM  ods.d_cocacola_score s
  INNER JOIN conf.d_cocacola_bottlers_mapping m
  ON s.bottler = m.bottlers_score
  WHERE bottler NOT IN ('Tier1','Tier2','Tier3','Sichuan Area / 四川区域','Tianjin Area / 天津区域','Liaoning / 辽宁');

