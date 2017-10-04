import csv 
import sys
import subprocess
import psycopg2	 

# pathDatos = '/home/hduser/Descargas/bigdata/bigdata-setdata.csv'
# pathNewDatos = '/home/hduser/Descargas/bigdata/data/'
pathDatos = '/home/ruben/dev/WebSites/input/big-data.csv'
pathNewDatos = '/home/ruben/dev/WebSites/input/'

try:
    conn = psycopg2.connect("dbname='bigdata' user='postgres' host='localhost' password='postgres'")
    conn.autocommit = True
except:
    print "I am unable to connect to the database"

enfermedades = {}
departamentos = {}
fechas = {}
result = 0

# Crear Tabla
querySql = """CREATE TABLE enfermedad (enfermedad TEXT, departamento TEXT, fecha TEXT, contador INTEGER)"""
cur = conn.cursor();
cur.execute(querySql)

f = open(pathDatos)
reader = csv.reader(f)
contador = 0
for row in reader:
	if contador !=0 :
		subprocess.call("mkdir " + pathNewDatos+row[0], shell=True)	
		with open(pathNewDatos+row[0]+'/'+row[0]+'.csv', 'a+') as newCSV :
			wr = csv.writer(newCSV, quoting=csv.QUOTE_ALL)
			wr.writerow(row)

		# Para agregar a la BD relacional
		try:
			# Se hace un select por ciudad y departamento 
			query = "SELECT contador FROM enfermedad_dpto WHERE departamento LIKE %(like)s AND enfermedad LIKE %(like_enf)s"
			cur.execute(query, dict(like= '%'+row[1]+'%', like_enf='%'+row[4]+'%'))
			resultados = cur.fetchall()
			if len(resultados) == 0:
				cur.execute('INSERT INTO enfermedad_dpto (fecha, departamento, enfermedad, contador) VALUES (%s, %s, %s, %s)', (row[0], row[1], row[4], 1))
			else:
				for resultado in resultados:
					if (resultado[0] != None):
						result = int(resultado[0]) + 1
						cur.execute('UPDATE enfermedad_dpto SET contador = %s WHERE departamento = %s AND enfermedad = %s AND fecha = %s', (result, row[1], row[4], row[0]))
		except psycopg2.Error as e:
			print e
			print "I can't update our test database!"

	contador+=1

# subprocess.call("hdfs dfs -mkdir input/medicina", shell=True)
# subprocess.call("hdfs dfs -put data input/medicina", shell=True)

f.close()