 --**********************************
 --* [INtro]
 --*   author=larluo@spiderdt.com
 --*   func=partition algorithm for data warehouse
 --*=================================
 --* [param]
 --*   tabname=stagINg table name
 --*   prt_cols_str=ods partition cols
 --*=================================
 --* [caller]
 --*   spark_etl_prt.groovy d_bolome_dau '[[":p_date", 1]]'
 --*   spark_etl_prt.groovy d_bolome_events
 --*   spark_etl_prt.groovy d_bolome_INventory
 --*   spark_etl_prt.groovy d_bolome_orders
 --*=================================
 --* [version]
 --*   v1_0=2016-09-28@larluo{create}
 --**********************************
  \c dw ;
  DROP VIEW model.d_cocacola_availability_rural ;
  CREATE OR REPLACE VIEW model.d_cocacola_availability_rural AS 
  WITH 
  product AS
    (SELECT 
      p.market,
      (CASE
      WHEN p.bg is null THEN 'ChINa ' ELSE p.bg END) AS bgs,
      (CASE
      WHEN p.bottler is null AND p.bg is not null THEN concat(p.bg,' Total ')
      WHEN p.bottler is null AND p.bg is null THEN 'ChINa Total '
      ELSE p.bottler END) AS bottlers,
      p.channel,
      p.product,
      p.category,
      p.brAND,
      p.period,
      p.total_availability,
      m.item 
      FROM ods.d_cocacola_product p,stg.d_cocacola_product_mappINg m 
      WHERE p.product = m.product
      AND p.channel = 'GT Urban'
      AND p.market NOT LIKE'ChINa Tier%'),
  avail AS
     (SELECT
       r.period,
       r.mbd,
       r.bg,
       r.bottler,
       r.channel,
       r.item,
       r.fact,
       po.product,
       (CASE
        WHEN r.item IN ('1.25/1.8L 美汁源果粒橙塑料瓶','450ml美汁源果粒奶优 塑料瓶','450ml 美之源果粒橙 塑料瓶','550ml 冰露水塑料瓶') THEN 'Still' ELSE 'SparklINg' END)          AS product_group,
       r.value,
       po.total_availability AS urban_value
     FROM ods.d_cocacola_availability_rural r,product po
     WHERE r.item = po.item 
     AND r.bg = po.bgs
     AND r.bottler = po.bottlers
     AND r.period = po.period)
     SELECT 
       period,
       mbd,
       bg,
       bottler,
       channel,
       item,
       fact,
       product,
       product_group,
       value,
       urban_value FROM avail;
