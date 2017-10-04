#encoding: utf-8
import happybase

connection = happybase.Connection('localhost')
table = connection.table('view')



row = table.row(b'Anorexia_02-12-2013_Boquerón')
print(row[b'cantidad:'])  # prints 'value1'

