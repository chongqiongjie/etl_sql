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
 --*   v1_1=2016-09-28@chong{modify drop table if exists}
 --**********************************
  \c dw ;
  DROP VIEW IF EXISTS model.d_cocacola_sku CASCADE;
  
  CREATE OR REPLACE VIEW model.d_cocacola_sku AS
  (with sku AS
  
  (select s.dw_dt,s.period,s.mbd,s.bottler_group,s.bottler,s.channel,
   unnest(array['0 SKU', '1 SKU', '2 SKU', '3 SKU', '4 SKU', '5 SKU', '6 SKU']) as sku_type,
   unnest(array[s.product0::NUMERIC(30,16), s.product1::NUMERIC(30,16),s.product2::NUMERIC(30,16), s.product3::NUMERIC(30,16),s.product4::NUMERIC(30,16),s.product5::NUMERIC(30,16),s.product6::NUMERIC(30,16)]) as product,
   m.abbrevation 
   from ods.d_cocacola_sku s
   LEFT JOIN conf.d_cocacola_bottlers_mapping m
   ON s.bottler = m.bottlers_en   


)

select * from sku);


