alias psql='/home/spiderdt/work/git/spiderdt-release/opt/pgsql/bin/psql "sslmode=require host=192.168.1.3 dbname=ms" -U ms'
export PGPASSWORD=spiderdt

echo "[printing stg.score]"
sh stg.d_cocacola_score.sh
echo "[printing ods.score]"
psql -f ods.d_cocacola_score.sql
echo "[printing model.score]"
psql -f model.d_cocacola_score.sql

echo "[printing stg.score_rural]"
sh stg.d_cocacola_score_rural.sh
echo "[printing ods.score_rural]"
psql -f ods.d_cocacola_score_rural.sql

echo "[printing stg.sku]"
sh stg.d_cocacola_sku.sh
echo "[printing ods.sku]"
psql -f ods.d_cocacola_sku.sql

echo "[printing stg.product]"
sh stg.d_cocacola_product.sh
echo "[printing ods.product]"
psql -f ods.d_cocacola_product.sql

echo "[printing stg.manufacture]"
sh stg.d_cocacola_manufacture.sh
echo "[printing ods.manufacture]"
psql -f ods.d_cocacola_manufacture.sql

echo "[printing stg.availability]"
sh stg.d_cocacola_availability.sh
echo "[printing ods.availability]"
psql -f  ods.d_cocacola_availability.sql

echo "[printing stg.availability_rural]"
sh stg.d_cocacola_availability_rural.sh



