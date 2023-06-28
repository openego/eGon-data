import psycopg2
import psycopg2.extras

hostname = 'localhost'
database = 'etrago-data'
username = 'postgres'
pwd = 'postgres'
port_id = 5432   
# normally using a configurationfile with this information

con=None
cur=None

try:
    conn = psycopg2.connect (
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id
    )

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)   # for searching the name of a column

    cur.execute('DROP TABLE IF EXISTS test') # for avoiding error, when you run script multiple times

    create_script = '''Create Table if NOT EXISTS test (
        id  int PRIMARY KEY,
        name   varchar(40) NOT NULL
    )'''

    cur.execute(create_script)

    insert_script = 'INSERT INTO test (id, name) VALUES (%s,%s)'
    insert_value = [(1, 'Marla'),(2, 'Lennart')]
    for record in insert_value:

        cur.execute(insert_script, record)

    update_script= 'Update test SET id = id*3'  # update data in a table
    cur.execute(update_script)

    delete_script = 'DELETE FROM test WHERE name = %s'
    delecte_record = ('Marla',)    
    cur.execute(delete_script,delecte_record)

    cur.execute('SELECT * FROM test')
    for record in cur.fetchall():
        print (record['id'],record['name'])



    conn.commit()


except Exception as error:
    print(error)

finally:           # we  need finally, because when there is error, cur and conn will be never closer
    if cur is not None:        # for avoiding an error if the connection never started
        cur.close()     #cursor is closed by ending the programm
    if conn is not None:
        conn.close()    #conn is closed by ending the programm
   