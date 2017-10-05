# encoding: utf-8
import happybase
import psycopg2

diagnostico = raw_input("Ingrese el diagnostico: ")
departamento = raw_input("Ingrese el departamento: ")
fecha = raw_input("Ingrese la fecha: ")

# Example = Anorexia_02-12-2013_Boquerón
queryHbase = "b'" + diagnostico + "_" + fecha + "_" + departamento + "'"

connection = happybase.Connection('localhost')
table = connection.table('enfermedad_dpto')
row = table.row(queryHbase)
resultHbase = row[b'cantidad:']
# print(row[b'cantidad:'])  # prints 'value1'

try:
    conn = psycopg2.connect("dbname='bigdata' user='postgres' host='localhost' password='postgres'")
    conn.autocommit = True
    try:
        cur = conn.cursor()
        query = "SELECT contador FROM enfermedad_dpto WHERE departamento = %s AND enfermedad = %s AND fecha = %s"
        cur.execute(query, (departamento, diagnostico, fecha))
        resultados = cur.fetchall()
        if len(resultados) == 0:
            print "No se encontraron resultados"
        else:
            for resultado in resultados:
                if (resultado[0] != None):
                    resultPostgres = int(resultado[0])
    except psycopg2.Error as e:
        print "Ocurrió un error al realizar la consulta en Postgres"
        print e
except:
    print "I am unable to connect to the database"

print "Cantidad de resultados encontrados: " + int(resultHbase + resultPostgres)
