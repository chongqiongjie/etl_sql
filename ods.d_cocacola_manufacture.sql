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
  CREATE TABLE IF NOT EXISTS stg.d_cocacola_manufacture_rng (
    dw_start_dt CHAR(10),
    dw_end_dt CHAR(10),
    dw_in_use CHAR(1),
    dw_ld_cnt INT,
    dw_ld_ts CHAR(22)
  ) ;

  CREATE TABLE IF NOT EXISTS ods.d_cocacola_manufacture (
    dw_dt CHAR(10),
    id                          TEXT,
    market                      TEXT,
    bg                          TEXT,
    bottler                     TEXT,
    channel                     TEXT,
    product                      TEXT,
    period                       TEXT,
    cooler_purity                TEXT,
    cooler_door                  TEXT,
    cooler_1st_position          TEXT,
    cooler_penetration           TEXT,
    cooler_inventory_rate        TEXT,
    act_avai_2nd_display         TEXT,
    activation_availability_mpoi TEXT,
    act_avai_floor_display       TEXT,
    activation_availability_g_end TEXT,
    act_avai_pillar_decoration   TEXT,
    act_avai_refreshment_center  TEXT,
    activation_sqm               TEXT,
    mpoi_sqm                     TEXT,
    floor_display_sqm            TEXT,
    g_end_sqm                    TEXT,
    pillar_decoration_sqm        TEXT,
    refreshment_center_sqm       TEXT,
    gt_activation_availability   TEXT,
    gt_full_cases_disp_avai      TEXT,
    gt_countertop_rack_avai      TEXT,
    gt_standing_rack_availability TEXT,
    gt_activation_number         TEXT,
    gt_full_cases_display_number TEXT,
    gt_countertop_rack_number    TEXT,
    gt_standing_rack_number      TEXT,
    ed_mh_act_avai               TEXT,
    ed_mh_merchandising_tools    TEXT,
    ed_mh_ko_case_or_bar_table   TEXT,
    ed_trad_act_avai             TEXT,
    ed_trad_rb_combo_promotion   TEXT,
    ed_trad_rb_case_display      TEXT,
    dm_penetration TEXT
  );
  
  update stg.d_cocacola_manufacture_rng set dw_in_use = '0' where dw_in_use = '1' ;
  insert into stg.d_cocacola_manufacture_rng (
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
  from stg.d_cocacola_manufacture ;

  delete from ods.d_cocacola_manufacture ods
  using stg.d_cocacola_manufacture_rng rng
  where rng.dw_in_use = '1'
  and ods.dw_dt between rng.dw_start_dt and rng.dw_end_dt ;

  insert into ods.d_cocacola_manufacture (
    dw_dt,
    id                         ,
    market                      ,
    bg                          ,
    bottler                     ,
    channel                     ,
    product                     ,
    period                      ,
    cooler_purity               ,
    cooler_door                 ,
    cooler_1st_position         ,
    cooler_penetration          ,
    cooler_inventory_rate       ,
    act_avai_2nd_display        ,
    activation_availability_mpoi,
    act_avai_floor_display      ,
    activation_availability_g_end,
    act_avai_pillar_decoration  ,
    act_avai_refreshment_center ,
    activation_sqm              ,
    mpoi_sqm                    ,
    floor_display_sqm           ,
    g_end_sqm                   ,
    pillar_decoration_sqm       ,
    refreshment_center_sqm      ,
    gt_activation_availability  ,
    gt_full_cases_disp_avai     ,
    gt_countertop_rack_avai     ,
    gt_standing_rack_availability,
    gt_activation_number        ,
    gt_full_cases_display_number,
    gt_countertop_rack_number   ,
    gt_standing_rack_number     ,
    ed_mh_act_avai              ,
    ed_mh_merchandising_tools   ,
    ed_mh_ko_case_or_bar_table  ,
    ed_trad_act_avai            ,
    ed_trad_rb_combo_promotion  ,
    ed_trad_rb_case_display     ,
    dm_penetration
  ) select
    TO_CHAR(TO_DATE(period, 'YYYYMM') + INTERVAL '1 MONTH - 1 day', 'YYYY-MM-DD') AS dw_dt,
     id                         ,
    market                      ,
    bg                          ,
    bottler                     ,
    channel                     ,
    product                     ,
    period                      ,                      
    cooler_purity               ,
    cooler_door                 ,
    cooler_1st_position         ,
    cooler_penetration          ,
    cooler_inventory_rate       ,
    act_avai_2nd_display        ,
    activation_availability_mpoi,
    act_avai_floor_display      ,
    activation_availability_g_end,
    act_avai_pillar_decoration  ,
    act_avai_refreshment_center ,
    activation_sqm              ,
    mpoi_sqm                    ,
    floor_display_sqm           ,
    g_end_sqm                   ,
    pillar_decoration_sqm       ,
    refreshment_center_sqm      ,
    gt_activation_availability  ,
    gt_full_cases_disp_avai     ,
    gt_countertop_rack_avai     ,
    gt_standing_rack_availability,
    gt_activation_number        ,
    gt_full_cases_display_number,
    gt_countertop_rack_number   ,
    gt_standing_rack_number     ,
    ed_mh_act_avai              ,
    ed_mh_merchandising_tools   ,
    ed_mh_ko_case_or_bar_table  ,
    ed_trad_act_avai            ,
    ed_trad_rb_combo_promotion  ,
    ed_trad_rb_case_display     ,
    dm_penetration
  from stg.d_cocacola_manufacture ;
