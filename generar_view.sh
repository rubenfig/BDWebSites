#!/bin/bash
sudo -i -u postgres
echo "drop table enfermedad" |psql
exit
echo "disable 'datos'; drop 'datos'; create 'datos','cantidad'" | hbase-1.3.1/bin/hbase shell
hadoop jar WebSites-1.0-SNAPSHOT.jar registros.SsJob /registros/* /output/medicina
~/hbase-1.3.1/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=,  -Dimporttsv.columns="HBASE_ROW_KEY,cantidad" datos hdfs:///output/medicina/part-r-00000
