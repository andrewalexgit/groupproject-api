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

	def all_subscriptions_by(column_name, data_value):	

		mydb = dbconnection()
		#create db cursor
		cursor = mydb.cursor(buffered=True)
		#sql statement
		sql = "SELECT * FROM subscriptions_vs WHERE {column_name} = '{data_value}'".format(**local())

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
		return json.dumps({'all_subscriptions for {data_value}':json_data}, default = myconverter)

	#input: email (string), password (hashed string), username (string), first (String), last (string), description (string), avatarUrl (string)
	#email and username must be unique (use find_user)
	#password should be hashed
	#all fields are required!!
	def add_subscription(username, port_id):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)

		sql = "INSERT INTO subscriptions (userId, portId) VALUES ((SELECT id FROM users WHERE username = '{username}'), {port_id})".format(**local())
		
		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})

		#close database connection
		cursor.close()
		mydb.close()
		
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return all_subscriptions('portId', portId)



	#input: username (string)
	def update_subscription(username, port_id, value):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)

		sql = "UPDATE subscriptions SET isActive = {value} WHERE userId = (SELECT id FROM users WHERE username = '{username}') and portId = {port_id}".format(**local())
		
		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})
		
		#close database connection
		cursor.close()
		mydb.close()
			
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return "subscription {username} to port {port_id} is updated".format(**local())


	def all_ports():

		mydb = dbconnection()
		#create db cursor
		cursor = mydb.cursor(buffered=True)
		#sql statement
		sql = '''SELECT id, name, description FROM ports where isActive = 1'''

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
		return json.dumps({'all_ports':json_data}, default = myconverter)

	def add_port(name, description):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql=  "INSERT INTO ports (name, description) VALUES ('{name}', '{description}')".format(**local())

		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})

		#close database connection
		cursor.close()
		mydb.close()
		
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return posts_db.all_ports()

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


	#input: column_name (string), data_value (string or int) 
	#options and types:
	#column_name: data_value
	#user_id: int
	#username: string
	#email: string
	#output: userid, username, email, first, last, avatarUrl
	#e.g. http://localhost:5000/find_users?column=username&value=chalshaff12
	#or http://localhost:5000/find_users?column=email&value=chalshaff12@gmail.com
	def find_users(column_name, data_value):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)

		sql = "SELECT * FROM users_vw WHERE {column_name} = '{data_value}'".format(**local())
		
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

		return json.dumps({'user':json_data}, default = myconverter)


	#input: email (string), password (hashed string), username (string), first (String), last (string), description (string), avatarUrl (string)
	#email and username must be unique (use find_user)
	#password should be hashed
	#all fields are required!!
	def add_user(email, password, username, first, last, avatarurl):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)

		sql = "INSERT INTO users (email, password, username, first, last, description, avatarurl) VALUES ('{email}','{password}','{username}','{first}','{last}', '{description}', '{avatarurl}')".format(**local())
		
		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})

		#close database connection
		cursor.close()
		mydb.close()
		
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return find_users('username', username)

#input: username (string)
	def update_user(username, column_name, value_name):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)

		sql = "UPDATE users SET {column_name} = '{value_name}' WHERE username = '{username}'".format(**local())
		
		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})
		
		#close database connection
		cursor.close()
		mydb.close()
			
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return find_users('username', username)

	#input: username (string)
	def delete_user(username):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)

		sql = "UPDATE users SET isActive = 0 WHERE username = '{username}'".format(**local())
		
		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})
		
		#close database connection
		cursor.close()
		mydb.close()
			
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return "user {username} deleted".format(**local())
		
	def all_posts_by(column_name, data_value):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql=  "SELECT * FROM posts_vw where {column_name} = '{data_value}'".format(**local())

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
		return json.dumps({'posts':json_data}, default = myconverter)	



	def add_post(title, text, port_id, author):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql=  "INSERT INTO posts (title, text, portid, userid) VALUES ('{title}','{text}',{port_id}, (select id from users where username = '{author}'))".format(**local())

		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})

		#close database connection
		cursor.close()
		mydb.close()
		
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return posts_db.all_posts_by('author', author)


	def delete_post(post_id):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql = "UPDATE posts SET isDeleted = 1 WHERE id = {post_id}".format(**local())

		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})
		
		#close database connection
		cursor.close()
		mydb.close()
			
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return "post {post_id} deleted"

	def update_post(post_id, column_name, data_value):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql=  "UPDATE posts SET {column_name} = '{data_value}' where id = {post_id}".format(**local())

		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})

		#close database connection
		cursor.close()
		mydb.close()
		
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return posts_db.all_posts_by('author', post_id)
	
	def all_comments_by(column_name, data_value):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql=  "SELECT * FROM comments_vw where {column_name} = '{data_value}'".format(**local())

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
		return json.dumps({'comments':json_data}, default = myconverter)	

	def add_comment(text, post_id, parent_id, author):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql=  "INSERT INTO comments ('{text}', {post_id}, {parent_id}, (select id from users where username = '{author}'))".format(**local())

		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})

		#close database connection
		cursor.close()
		mydb.close()
		
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return posts_db.all_comments('author', author)


	def delete_comment(comment_id):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql = "UPDATE comments SET isDeleted = 1 WHERE id = {comment_id}".format(**local())

		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})
		
		#close database connection
		cursor.close()
		mydb.close()
			
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return "comment {comment_id} deleted"

	def update_comment(comment_id, text):

		mydb = dbconnection()
		cursor = mydb.cursor(buffered=True)
		sql=  "UPDATE comments SET text = '{text}' where id = {comment_id}".format(**local())

		try:
			cursor.execute(sql)
			mydb.commit()
		except mysql.connector.Error as err:
			return json.dumps({'error':str(err)})

		#close database connection
		cursor.close()
		mydb.close()
		
		def myconverter(o):
			if isinstance(o, datetime.datetime):
				return o.__str__()

		return posts_db.all_comments_by('comment_id', comment_id)
