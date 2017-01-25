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

DROP VIEW model.d_cocacola_activation_summary ;

CREATE OR REPLACE VIEW model.d_cocacola_activation_summary AS
WITH manufacture AS (
  SELECT
    dw_dt,
    market,
    bg,
    bottler,
    channel,
    (CASE WHEN product = 'TCCC' THEN 'KO'
          WHEN product = 'Ting Hsin & PBI' THEN 'KB' END) AS vendor_base,
    UNNEST(ARRAY['Activation_Penetration','Actiavtion_SQM']) AS fact,
    UNNEST(ARRAY[act_avai_2nd_display, activation_sqm]) AS value_base
  FROM ods.d_cocacola_manufacture
  WHERE product IN ('TCCC','Ting Hsin & PBI') and market NOT LIKE 'China Tier%'
), activation_summary AS (
  SELECT
    dw_dt,
    market,
    bg,
    bottler,
    channel,
    UNNEST(ARRAY['KO', 'KB', 'KO/KB']) AS vendor,
    fact,
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
    fact
)
SELECT * FROM activation_summary;
