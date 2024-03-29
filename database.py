import mysql.connector
import json
import datetime

def dbconnection():

	#localhost
	# mydb = mysql.connector.connect(
	# 		host="localhost",
	# 		user="root",
	# 		passwd="",
	# 		database="dbtest"
	# )


	#webhosteddb
	mydb = mysql.connector.connect(
		host="35.239.141.59",
		user="backendteam",
		passwd="UZSDmp7J2J2ZYHw",
	#test db
		database="test_db"
	#deploymentdb
	#   database="cisc3140"
	)

	return mydb

def all_users():	

	mydb = dbconnection()
	#create db cursor
	cursor = mydb.cursor(buffered=True)
	#sql statement
	sql = '''SELECT * FROM users_vw'''

	try:
		cursor.execute(sql)
	except mysql.connector.Error as err:
		return json.dumps({'error':str(err)})

	result_set = cursor.fetchall()		#save sql result set
	#convert columns and rows into json data
	json_data = [dict(zip([key[0] for key in cursor.description], row)) for row in result_set]
	#close database connection

	cursor.close()
	mydb.close()

	#catch datetime datatype error for json
	def myconverter(o):
		if isinstance(o, datetime.datetime):
			return o.__str__()
	return json.dumps({'all_users':json_data}, default = myconverter)

def add_user(email, password, username, first, last, avatarurl):

	try:
		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
                description = "Regular user"

		sql = "INSERT INTO users (email, password, username, first, last, description, avatarurl) VALUES ('{email}','{password}','{username}','{first}','{last}', '{description}', '{avatarurl}')".format(**local())
	
		cursor.execute(sql)
		mydb.commit()
	except Exception  as err:
		return json.dumps({'error':str(err), 'module':'db' })

	#close database connection
	cursor.close()
	mydb.close()
	
	def myconverter(o):
		if isinstance(o, datetime.datetime):
			return o.__str__()

	return username
