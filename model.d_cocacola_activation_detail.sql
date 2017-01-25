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
 --*   spark_etl_prt.groovy d_bolome_dau '[[':p_date', 1]]'
 --*   spark_etl_prt.groovy d_bolome_events
 --*   spark_etl_prt.groovy d_bolome_inventory
 --*   spark_etl_prt.groovy d_bolome_orders
 --*=================================
 --* [version]
 --*   v1_0=2016-09-28@larluo{create}
 --**********************************

\c dw ;

DROP VIEW model.d_cocacola_activation_detail ;

CREATE OR REPLACE VIEW model.d_cocacola_activation_detail AS
WITH manufacture AS (
  SELECT
    dw_dt,
    market,
    bg,
    bottler,
    channel,
    (CASE WHEN product = 'TCCC' THEN 'KO'
          WHEN product = 'Ting Hsin & PBI' THEN 'KB' END) AS vendor_base,
    UNNEST(ARRAY['activation_availability_mpoi', 'act_avai_floor_display', 'activation_availability_g_end', 'act_avai_pillar_decoration', 'act_avai_refreshment_center',
                 'mpoi_sqm', 'floor_display_sqm', 'g_end_sqm', 'pillar_decoration_sqm', 'refreshment_center_sqm']) AS fact_base,
    UNNEST(ARRAY[activation_availability_mpoi, act_avai_floor_display, activation_availability_g_end, act_avai_pillar_decoration, act_avai_refreshment_center,
                 mpoi_sqm, floor_display_sqm, g_end_sqm, pillar_decoration_sqm, refreshment_center_sqm]) AS value_base
  FROM ods.d_cocacola_manufacture
  WHERE product IN ('TCCC','Ting Hsin & PBI') and market NOT LIKE 'China Tier%'
), activation_detail AS (
  SELECT
    dw_dt,
    market,
    bg,
    bottler,
    channel,
    UNNEST(ARRAY['KO', 'KB', 'KO/KB']) AS vendor,
    (CASE
      WHEN fact_base IN (
        'activation_availability_mpoi', 'act_avai_floor_display', 'activation_availability_g_end', 'act_avai_pillar_decoration', 'act_avai_refreshment_center'
      ) THEN 'Penetration'
      WHEN fact_base IN (
        'mpoi_sqm', 'floor_display_sqm', 'g_end_sqm', 'pillar_decoration_sqm', 'refreshment_center_sqm'
      ) THEN 'SQM'
    END) AS detail,
    (CASE
      WHEN fact_base IN('activation_availability_mpoi', 'mpoi_sqm') THEN 'MPOI'
      WHEN fact_base IN('act_avai_floor_display','floor_display_sqm') THEN 'Floor Display'
      WHEN fact_base IN('activation_availability_g_end','g_end_sqm') THEN 'G End'
      WHEN fact_base IN('act_avai_pillar_decoration','pillar_decoration_sqm') THEN 'Pillar Decoration'
      WHEN fact_base IN('act_avai_refreshment_center','refreshment_center_sqm') THEN 'Refreshment Center'
    END) AS fact,
    UNNEST(ARRAY[
      SUM(CASE WHEN vendor_base = 'KO' THEN value_base::NUMERIC(30,16) END),
      SUM(CASE WHEN vendor_base = 'KB' THEN value_base::NUMERIC(30,16) END),
      SUM(CASE WHEN vendor_base = 'KO' THEN value_base::NUMERIC(30,16) END)/NULLIF(SUM(CASE WHEN vendor_base = 'KB' THEN value_base::NUMERIC(30,16) END),0)
    ]) AS value
  FROM manufacture
  GROUP BY
    dw_dt,
    market,
    bg,
    bottler,
    channel,
    fact_base
)
SELECT * FROM activation_detail ;
