\c dw;

DROP VIEW model.d_cocacola_availability_rural;

CREATE OR REPLACE VIEW model.d_cocacola_availability_rural AS
(with 
product as
(select 
  p.dw_dt,
  (CASE
  WHEN p.bg is null THEN 'China ' ELSE p.bg END) AS bgs,
  (CASE
  WHEN p.bottler is null and p.bg is not null THEN concat(p.bg,' Total ')
  WHEN p.bottler is null and p.bg is null THEN 'China Total '
  ELSE p.bottler END) AS bottlers,
  p.channel,
  m.item ,
  (CASE
   WHEN m.item in ('1.25/1.8L 美汁源果粒橙塑料瓶','450ml美汁源果粒奶优 塑料瓶','450ml 美之源果粒橙 塑料瓶','550ml 冰露水塑料瓶') THEN 'Still' 
   ELSE 'Sparkling' END) AS product_group,
  p.total_availability::NUMERIC(30,16) as value
 
  from ods.d_cocacola_product p,conf.d_cocacola_product_mapping m 
where p.product = m.product
and p.channel = 'GT Urban'
and p.market not like 'China Tier%'
and p.dw_dt in (select DISTINCT dw_dt from ods.d_cocacola_availability_rural)

union 
select 
  r.dw_dt,
  r.bg,
  r.bottler,
  r.channel,
  r.item,
  (CASE
   WHEN r.item in ('1.25/1.8L 美汁源果粒橙塑料瓶','450ml美汁源果粒奶优 塑料瓶','450ml 美之源果粒橙 塑料瓶','550ml 冰露水塑料瓶') THEN 'Still' 
   ELSE 'Sparkling' END) AS product_group,
  r.value::NUMERIC(30,16)*100
from ods.d_cocacola_availability_rural r
where r.mbd not like 'Tier%'
and r.bottler not in ('Sichuan Area','Tianjin Area','Liaoning'))

select * from product);
