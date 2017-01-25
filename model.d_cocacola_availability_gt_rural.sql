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
 --*   v1_0=2016-09-28@chong{create}
 --**********************************
\c dw;
DROP VIEW IF EXISTS model.d_cocacola_availability_gt_rural CASCADE;

CREATE OR REPLACE VIEW model.d_cocacola_availability_gt_rural AS SELECT total_availability FROM ods.d_cocacola_product WHERE product = 'NARTD Juice TCCC 1.25L/1.8L PET (MMPO)' AND dw_dt = '2016-10-31' AND bg  IS NULL  AND bottler IS NULL AND channel = 'GT Urban'

