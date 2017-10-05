#!/bin/bash
sudo -i -u postgres
echo "drop table enfermedad_dpto" | psql
exit
echo "disable 'enfermedad_dpto'; drop 'enfermedad_dpto'; create 'enfermedad_dpto','cantidad'" | hbase-1.3.1/bin/hbase shell
hadoop jar WebSites-1.0-SNAPSHOT.jar registros.SsJob /input/medicina/* /output/medicina
~/hbase-1.3.1/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=,  -Dimporttsv.columns="HBASE_ROW_KEY,cantidad" enfermedad_dpto hdfs:///output/medicina/part-r-00000
