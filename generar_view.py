import subprocess
import psycopg2

try:
    conn = psycopg2.connect("dbname='bigdata' user='postgres' host='localhost' password='postgres'")
    conn.autocommit = True
except:
    print "I am unable to connect to the database"

queryDropTable = "DROP TABLE enfermedad_dpto"
cur = conn.cursor()
cur.execute(queryDropTable)
subprocess.call("echo \"disable 'enfermedad_dpto'; drop 'enfermedad_dpto'; create 'enfermedad_dpto','cantidad'\" | hbase-1.3.1/bin/hbase shell hadoop jar WebSites-1.0-SNAPSHOT.jar registros.SsJob /input/medicina/* /output/medicina", shell=True)
subprocess.call("~/hbase-1.3.1/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=,  -Dimporttsv.columns=\"HBASE_ROW_KEY,cantidad\" enfermedad_dpto hdfs:///output/medicina/part-r-00000", shell=True)
