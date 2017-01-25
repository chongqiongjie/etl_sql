# 1. convert excel to csv
libreoffice --headless --convert-to csv --infilter=CSV:44,34,UTF8 --outdir ~/work/git/spiderdt-team/var/data/cocacola/ ~/work/git/spiderdt-team/var/data/cocacola/6SKU-availability-所有期.xls

# upload to hdfs for backup
# hdfs dfs -mkdir -p /user/hive/warehouse/stg_bk.db/d_cocacola_sku/$(date '+%Y-%m-%d')
# hdfs dfs -put /home/spiderdt/work/git/spiderdt-team/var/data/cocacola/6SKU-availability-所有期.csv /user/hive/warehouse/stg_bk.db/d_cocacola_sku/$(date '+%Y-%m-%d')/data.csv

CURRENT_DT=$(date '+%Y-%m-%d')
# 2. upload to lfs for backup
mkdir -p /home/spiderdt/work/git/spiderdt-team/var/data/stg_bk.db/d_cocacola_sku/${CURRENT_DT}
cp /home/spiderdt/work/git/spiderdt-team/var/data/cocacola/6SKU-availability-所有期.csv /home/spiderdt/work/git/spiderdt-team/var/data/stg_bk.db/d_cocacola_sku/${CURRENT_DT}/data.csv

# 3. prepare current period 
rm -rf /home/spiderdt/work/git/spiderdt-team/var/data/stg.db/d_cocacola_sku/*
mkdir -p /home/spiderdt/work/git/spiderdt-team/var/data/stg.db/d_cocacola_sku/${CURRENT_DT}
cp /home/spiderdt/work/git/spiderdt-team/var/data/stg_bk.db/d_cocacola_sku/${CURRENT_DT}/data.csv /home/spiderdt/work/git/spiderdt-team/var/data/stg.db/d_cocacola_sku/${CURRENT_DT}/data.csv

# 4. load to pgsql
/home/spiderdt/work/git/spiderdt-release/opt/pgsql/bin/psql "sslmode=require host=192.168.1.3 dbname=ms" -U ms <<EOF
   \c dw ;
   CREATE TABLE IF NOT EXISTS stg.d_cocacola_sku (
     period TEXT,
     mbd TEXT,
     bottler_group TEXT,
     bollter TEXT,
     channel TEXT,
     product0 TEXT,
     product6 TEXT,
     product1 TEXT,
     product2 TEXT,
     product3 TEXT,
     product4 TEXT,
     product5 TEXT
   ) ;
   truncate table stg.d_cocacola_sku ;
   \copy stg.d_cocacola_sku FROM '/home/spiderdt/work/git/spiderdt-team/var/data/stg.db/d_cocacola_sku/2017-01-02/data.csv' CSV HEADER ;
EOF


SET search_path TO stg,ods ;
