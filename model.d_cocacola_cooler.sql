\c dw;
DROP VIEW model.d_cocacola_cooler;
CREATE OR REPLACE VIEW model.d_cocacola_cooler AS WITH 
manufacture AS (
  SELECT   
    dw_dt   ,
    market  ,
    bg      ,
    bottler ,
    channel ,
    (CASE
       WHEN product = 'TCCC' THEN 'KO'
       WHEN product = 'Ting Hsin & PBI' THEN 'KB'
    END) AS vendor,
    UNNEST(ARRAY['cooler_purity','cooler_door','cooler_1st_position','cooler_penetration','cooler_inventory_rate']) AS fact,
    UNNEST(ARRAY[cooler_purity::NUMERIC(30,16),cooler_door::NUMERIC(30,16),cooler_1st_position::NUMERIC(30,16),cooler_penetration::NUMERIC(30,16),cooler_inventory_rate::NUMERIC(30,16)]) AS value
  FROM ods.d_cocacola_manufacture 
  WHERE product IN ('TCCC', 'Ting Hsin & PBI') and market NOT LIKE 'China Tier%'
) select * from manufacture ;
