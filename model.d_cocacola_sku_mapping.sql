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
  DROP VIEW IF EXISTS model.d_cocacola_sku_mapping CASCADE;

  CREATE OR REPLACE VIEW model.d_cocacola_sku_mapping AS
  SELECT p.dw_dt,p.period,p.market,p.bg,p.bottler,p.channel,s.category as sku,s.brand,s.sku as sku_detail,s.orders, p.total_availability::NUMERIC(30,16) as value
  FROM conf.d_cocacola_sku_mapping s
    INNER JOIN ods.d_cocacola_product p
    ON s.mapping_raw_data = p.product
    WHERE p.market NOT LIKE 'China Tier%';

  
  

