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
 --*   v1_1=2017-01-15@jing{modify}
  --*  v1_2=2017-01-25@jing{modify}
 --**********************************

  \c dw ;

DROP VIEW model.d_cocacola_sovi;

CREATE OR REPLACE VIEW model.d_cocacola_sovi AS
WITH product AS(
    SELECT
      dw_dt, bg, bottler, channel, market,
      (CASE
        WHEN product = 'NARTD Sparkling TCCC' THEN 'KO-Sparkling'
        WHEN product = 'NARTD Juice TCCC' THEN 'KO-Juice'
        WHEN product = 'NARTD PW TCCC' THEN 'KO-PW'
        WHEN product = 'NARTD RTDT TCCC' THEN 'KO-RTDT'
        WHEN product = 'NARTD VAD TCCC' THEN 'KO-VAD'
        WHEN product LIKE 'NARTD%' AND manu IS NULL THEN 'TOTAL-NARTD'
        WHEN product = 'NARTD Sparkling PBI' THEN 'KB-Sparkling'
        WHEN product IN ('NARTD Juice PBI', 'NARTD Juice Ting Hsin') THEN 'KB-Juice'
        WHEN product IN ('NARTD PW PBI', 'NARTD PW Ting Hsin') THEN 'KB-PW'
        WHEN product = 'NARTD RTDT Ting Hsin' THEN 'KB-RTDT'
      END) vendor_product_category,
      total_sovi AS origin_total_sovi,
      shelf_sovi AS origin_shelf_sovi,
      cooler_sovi AS origin_cooler_sovi,
      total_facing_number AS origin_total_facing_number,
      shelf_facing_number AS origin_shelf_facing_number,
      cooler_facing_number AS origin_cooler_facing_number
    FROM ods.d_cocacola_product ods
    WHERE market NOT LIKE 'China Tier%'
  ),

  sovi AS (
    (
      SELECT
          dw_dt, bg, bottler, channel, market,
          SPLIT_PART(vendor_product_category, '-', 1) as vendor,
          SPLIT_PART(vendor_product_category, '-', 2) as product_category,
          SUM(origin_total_sovi::NUMERIC(30,16)) AS total_sovi,
          SUM(origin_shelf_sovi::NUMERIC(30,16)) AS shelf_sovi,
          SUM(origin_cooler_sovi::NUMERIC(30,16)) AS cooler_sovi
        FROM product
        WHERE vendor_product_category IS NOT NULL
        GROUP BY 
          dw_dt, bg, bottler, channel, market,
          SPLIT_PART(vendor_product_category, '-', 1),
          SPLIT_PART(vendor_product_category, '-', 2)
    )

    UNION ALL

    (
      SELECT 
          dw_dt, bg, bottler, channel, market,
          SPLIT_PART(vendor_product_category, '-', 1) as vendor,
          'NARTD' AS product_category,

          (CASE
            WHEN SPLIT_PART(vendor_product_category, '-', 1) != 'TOTAL'
            THEN SUM(SUM(origin_total_facing_number::NUMERIC(30,16)))
                 OVER( PARTITION BY dw_dt, bg, bottler, channel, market, SPLIT_PART(vendor_product_category, '-', 1)
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                 ) / SUM(CASE WHEN SPLIT_PART(vendor_product_category, '-', 1) = 'TOTAL' 
                              THEN SUM(origin_total_facing_number::NUMERIC(30,16)) END) 
                     OVER( PARTITION BY dw_dt, bg, bottler, channel, market 
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                     ) * 100
          END) AS total_sovi,

          (CASE
            WHEN SPLIT_PART(vendor_product_category, '-', 1) != 'TOTAL'
            THEN SUM(SUM(origin_shelf_facing_number::NUMERIC(30,16)))
                 OVER( PARTITION BY dw_dt, bg, bottler, channel, market, SPLIT_PART(vendor_product_category, '-', 1)
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                 ) / SUM(CASE WHEN SPLIT_PART(vendor_product_category, '-', 1) = 'TOTAL' 
                              THEN SUM(origin_shelf_facing_number::NUMERIC(30,16)) END) 
                     OVER( PARTITION BY dw_dt, bg, bottler, channel, market 
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                     ) * 100
          END) AS shelf_sovi,

          (CASE
            WHEN SPLIT_PART(vendor_product_category, '-', 1) != 'TOTAL'
            THEN SUM(SUM(origin_cooler_facing_number::NUMERIC(30,16)))
                 OVER( PARTITION BY dw_dt, bg, bottler, channel, market, SPLIT_PART(vendor_product_category, '-', 1)
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                 ) / SUM(CASE WHEN SPLIT_PART(vendor_product_category, '-', 1) = 'TOTAL' 
                              THEN SUM(origin_cooler_facing_number::NUMERIC(30,16)) END) 
                     OVER( PARTITION BY dw_dt, bg, bottler, channel, market 
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                     ) * 100
            END) AS cooler_sovi
          FROM product
          WHERE vendor_product_category IS NOT NULL
          GROUP BY 
            dw_dt, bg, bottler, channel, market,
            SPLIT_PART(vendor_product_category, '-', 1)
      )

    ),

    result AS(
      SELECT
          dw_dt, bg, bottler, channel, market,vendor, product_category,
                    unnest(array['Total','Shelf','Cooler']) AS sovi_type,
          unnest(array[total_sovi, shelf_sovi, cooler_sovi]) AS value
      FROM sovi
      WHERE vendor != 'TOTAL'
      )

 SELECT * FROM result;
