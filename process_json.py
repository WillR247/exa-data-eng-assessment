import json
import mysql.connector

def connect_to_database(db):
    try:
        return mysql.connector.connect(
            host="mysqldb",
            user="root",
            password="root",
            database=db
        )
    except:
        print("ERROR: Patient Database does not exist")
        return False

def create_database():
    # Checks if the database already exists and therefore doesn't need to be created
    # If the database does not already exist, begins creating the database
    if connect_to_database("patient_data") == False:
        print("Creating new patient database...\n")
        con = mysql.connector.connect(
            host="mysqldb",
            user="root",
            password="root"
        )
        db_cursor = con.cursor()
        db_cursor.execute("CREATE DATABASE patient_data")

        # Closes the previous connection and establishes a new connection to the newly created database
        con.close()
        new_con = connect_to_database("patient_data")
        db_cursor = new_con.cursor()

        # Creates the patient table
        db_cursor.execute("CREATE TABLE patient ("
            "patient_id INT AUTO_INCREMENT PRIMARY KEY, uuid VARCHAR(255), "
            "first_name VARCHAR(255), surname VARCHAR(255), prefix VARCHAR(255), "
            "gender VARCHAR(255), birth_sex VARCHAR(255), "
            "birth_date VARCHAR(255), "
            "address_line VARCHAR(255), address_city VARCHAR(255), address_state VARCHAR(255), address_country VARCHAR(255), "
            "address_lat DECIMAL(10,8), address_long DECIMAL(11,8), "
            "birth_city VARCHAR(255), birth_state VARCHAR(255), birth_country VARCHAR(255), "
            "ethnicity VARCHAR(255), race VARCHAR(255), "
            "phone_number VARCHAR(255), language VARCHAR(255), "
            "mother_maiden VARCHAR(255), "
            "marital_status VARCHAR(255), "
            "death_datetime VARCHAR(255))")
        
        # Creates a table containing the patient's forms of identification, as each patient has many forms of identification
        db_cursor.execute("CREATE TABLE patient_identification ("
            "patient_identification_id INT AUTO_INCREMENT PRIMARY KEY, patient INT, "
            "id_system VARCHAR(255), "
            "type VARCHAR(255), "
            "value VARCHAR(255), "
            "FOREIGN KEY(patient) REFERENCES patient(patient_id))")

        # Creates a table containing all the event data for each patient. Currently just stores most event data as a longtext, ideally will try to separate that out in future
        db_cursor.execute("CREATE TABLE patient_event ("
            "patient_event_id INT AUTO_INCREMENT PRIMARY KEY, patient INT, "
            "event_type VARCHAR(255), "
            "event_data LONGTEXT, "
            "FOREIGN KEY(patient) REFERENCES patient(patient_id))")
        
        new_con.close()
        print("Successfully created patient database!\n")

con = mysql.connector.connect(
            host="mysqldb",
            user="root",
            password="root"
        )
db_cursor = con.cursor()
db_cursor.execute("DROP DATABASE patient_data")
create_database()