import json
import mysql.connector

def create_database():
    con = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="root"
    )
    print(con)

create_database()