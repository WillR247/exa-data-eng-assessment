import json
import mysql.connector
import os

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

def add_patient(patient_data):
    con = connect_to_database("patient_data")
    db_cursor = con.cursor()

    # Sets the more complex data points to variables to reduce SQL complexity
    first_name = patient_data['name'][0]['given'][0]
    surname = patient_data['name'][0]['family']
    try:
        prefix = patient_data['name'][0]['prefix'][0]
    except:
        prefix = ""

    uuid = patient_data['id']
    birth_date = patient_data['birthDate']
    gender = patient_data['gender']
    birth_sex = patient_data['extension'][3]['valueCode']
    mother_maiden = patient_data['extension'][2]['valueString']
    marital_status = patient_data['maritalStatus']['text']
    birth_city = patient_data['extension'][4]['valueAddress']['city']
    birth_state = patient_data['extension'][4]['valueAddress']['state']
    birth_country = patient_data['extension'][4]['valueAddress']['country']
    ethnicity = patient_data['extension'][1]['extension'][1]['valueString']
    race = patient_data['extension'][0]['extension'][1]['valueString']
    try:
        death_datetime = patient_data['deceasedDateTime']
    except:
        death_datetime = "N/A"

    address_line = patient_data['address'][0]['line'][0]
    address_city = patient_data['address'][0]['city']
    address_state = patient_data['address'][0]['state']
    address_country = patient_data['address'][0]['country']
    address_lat = patient_data['address'][0]['extension'][0]['extension'][0]['valueDecimal']
    address_long = patient_data['address'][0]['extension'][0]['extension'][1]['valueDecimal']

    phone_number = patient_data['telecom'][0]['value']
    language = patient_data['communication'][0]['language']['text']

    db_cursor.execute("INSERT INTO patient (uuid, "
    "first_name, surname, prefix, "
    "gender, birth_sex, birth_date, "
    "address_line, address_city, address_state, address_country, address_lat, address_long, "
    "birth_city, birth_state, birth_country, "
    "ethnicity, race, mother_maiden, marital_status, "
    "phone_number, language, "
    "death_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",[
        uuid,
        first_name, surname, prefix, 
        gender, birth_sex, birth_date, 
        address_line, address_city, address_state, address_country, address_lat, address_long,
        birth_city, birth_state, birth_country, 
        ethnicity, race, mother_maiden, marital_status, 
        phone_number, language, 
        death_datetime])
    return "temp"

def process_json_data(json_data, file_name):
    # Verifies that the file is using the correct format
    if json_data['entry'][0]['resource']['resourceType'] == "Patient":
        patient_data = json_data['entry'][0]['resource']

        # Creates a connection to the database, and checks if the patient's unique ID is already in the patient table, to avoid duplication
        con = connect_to_database("patient_data")
        db_cursor = con.cursor()
        db_cursor.execute("SELECT * FROM patient WHERE uuid = %s",[patient_data['id']])

        if len(db_cursor.fetchall()) == 0:
            #Send data to add_patient function, return the patient id
            db_cursor.close()
            patient_id = add_patient(patient_data)

            for entry in json_data['entry'][1:]:
                event_data = entry['resource']
                # add_patient_event(event_data, patient_id)

        else:
            print("Patient data already stored in database\n")
    else:
        print("ERROR: File {} is using an incorrect format\n".format(file_name))

def load_json_data():
    # Loads the filenames in the data directory into an array
    files = os.listdir("data")
    print("Loading {} files from data directory...".format(len(files)))

    # Opens each file, sending the json data contained to the process_json_data function
    for file in files:
        with open("data/{}".format(file), "r") as f:
            print("Processing file - {}".format(file))
            try:
                data = f.read()
                process_json_data(json.loads(data), file)
            except:
                print("ERROR: Could not process file {}\n".format(file))

con = mysql.connector.connect(
            host="mysqldb",
            user="root",
            password="root"
        )
db_cursor = con.cursor()
db_cursor.execute("DROP DATABASE patient_data")
create_database()
load_json_data()