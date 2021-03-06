import csv
import sys
import subprocess
import psycopg2
from cStringIO import StringIO


fileName = raw_input("Ingrese el nombre del archivo fuente: ")
#pathFileName = '/home/carlitos/Descargas/bigdata/'
pathFileName = '/home/hduser/bigdata/'
pathDatos = pathFileName + fileName

#pathNewDatos = '/home/carlitos/Descargas/bigdata/data/'
pathNewDatos = '/home/hduser/bigdata/data/'


try:
    conn = psycopg2.connect("dbname='bigdata' user='postgres' host='localhost' password='postgres'")
    conn.autocommit = True

    enfermedades = {}
    departamentos = {}
    fechas = {}
    result = 0

    # Crear Tabla
    querySql = """CREATE TABLE IF NOT EXISTS enfermedad_dpto (enfermedad TEXT, departamento TEXT, fecha TEXT, contador INTEGER)"""
    queryDropTable = "DROP TABLE enfermedad_dpto"
    cur = conn.cursor()
    #cur.execute(queryDropTable)
    cur.execute(querySql)

    f = open(pathDatos)
    reader = csv.reader(f)
    contador = 0
    for row in reader:
        if contador != 0:
            data = StringIO()
            wr = csv.writer(data, quoting=csv.QUOTE_NONE, escapechar='\\')
            wr.writerow(row)
            result = subprocess.call("/usr/local/hadoop/bin/hdfs dfs -mkdir /input/medicina/" + row[0], shell=True)
            if result == 0:
                    subprocess.call("echo '"+ data.getvalue().replace("\n", "") +"' | /usr/local/hadoop/bin/hdfs dfs -put - /input/medicina/" + row[0] + "/" + row[0] + ".csv", shell=True)
            else:
                subprocess.call("echo '"+ data.getvalue().replace("\n", "") +"' | /usr/local/hadoop/bin/hdfs dfs -appendToFile - /input/medicina/" + row[0] + "/" + row[0] + ".csv", shell=True)



            # Para agregar a la BD relacional
            try:
                # Se hace un select por ciudad y departamento
                query = "SELECT contador FROM enfermedad_dpto WHERE departamento = %s AND enfermedad = %s AND fecha = %s"
                cur.execute(query, (row[1], row[4], row[0]))
                resultados = cur.fetchall()
                if len(resultados) == 0:
                    cur.execute(
                        'INSERT INTO enfermedad_dpto (fecha, departamento, enfermedad, contador) VALUES (%s, %s, %s, %s)',
                        (row[0], row[1], row[4], 1))
                else:
                    for resultado in resultados:
                        if (resultado[0] != None):
                            result = int(resultado[0]) + 1
                            cur.execute(
                                'UPDATE enfermedad_dpto SET contador = %s WHERE departamento = %s AND enfermedad = %s AND fecha = %s',
                                (result, row[1], row[4], row[0]))
            except psycopg2.Error as e:
                print e
                print "I can't update our test database!"

        contador += 1

    f.close()
except Exception as e:
    print "Ocurrio un Error: "
    print e