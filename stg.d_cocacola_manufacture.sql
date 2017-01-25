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
  CREATE TABLE stg.d_cocacola_manufacture (
    id TEXT, 
    market TEXT, 
    bg TEXT, 
    bottler TEXT, 
    channel TEXT, 
    product TEXT, 
    period TEXT, 
    cooler_purity TEXT, 
    cooler_door TEXT, 
    cooler_1st_position TEXT, 
    cooler_penetration TEXT,
    cooler_inventory_rate TEXT, 
    act_avai_2nd_display TEXT, 
    activation_availability_mpoi TEXT, 
    act_avai_floor_display TEXT, 
    activation_availability_g_end TEXT, 
    act_avai_pillar_decoration TEXT, 
    act_avai_refreshment_center TEXT, 
    activation_sqm TEXT, mpoi_sqm TEXT,
    floor_display_sqm TEXT, 
    g_end_sqm TEXT, 
    pillar_decoration_sqm TEXT, 
    refreshment_center_sqm TEXT, 
    gt_activation_availability TEXT, 
    gt_full_cases_disp_avai TEXT,
    gt_countertop_rack_avai TEXT, 
    gt_standing_rack_availability TEXT, 
    gt_activation_number TEXT, 
    gt_full_cases_display_number TEXT, 
    gt_countertop_rack_number TEXT,
    gt_standing_rack_number TEXT, 
    ed_mh_act_avai TEXT, 
    ed_mh_merchandising_tools TEXT, 
    ed_mh_ko_case_or_bar_table TEXT, 
    ed_trad_act_avai TEXT, 
    ed_trad_rb_combo_promotion TEXT,
    ed_trad_rb_case_display TEXT, 
    dm_penetration TEXT
    );
  truncate table stg.d_cocacola_manufacture;
  \copy stg.d_cocacola_manufacture FROM '/home/spiderdt/work/git/spiderdt-team/var/data/stg.db/d_cocacola_manufacture/__DATA__.csv' CSV HEADER ;

