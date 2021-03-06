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
  CREATE TABLE IF NOT EXISTS  stg.d_cocacola_score (
    PERIOD text, 
    MBD text, 
    BG text, 
    BOTTLER text, 
    CHANNEL text, 
    CODE text, 
    Item text, 
    Fact text, 
    Value text
    );
  truncate table stg.d_cocacola_score ;
  \copy stg.d_cocacola_score FROM '/home/spiderdt/work/git/spiderdt-team/var/data/stg.db/d_cocacola_score/__DATA__.csv' CSV HEADER ;
