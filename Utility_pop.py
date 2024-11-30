import csv
import os
import pyodbc
from datetime import datetime


### FUNZIONI PER IL POPOLAMENTO

# Connect to the database
def connect_to_db():
    server = 'lds.di.unipi.it'
    db = 'Group_ID_24_DB'
    username = 'Group_ID_24'
    password = 'IMTGP44N'
    driver = 'ODBC Driver 17 for SQL Server'

    try:
        connection = pyodbc.connect(
            f'DRIVER={{{driver}}};'
            f'SERVER={server};'
            f'DATABASE={db};'
            f'UID={username};'
            f'PWD={password}'
        )
        print("Database connection successful!")
        return connection
    except Exception as e:
        print("Error connecting to database:", e)
        return None

# Read and insert CSV data
def populate_database(file_path, table_name, connection):
    import csv
    try:
        # Open the CSV file
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)

            # Get the column names from the first row
            csv_columns = next(reader)  # Legge l'intestazione

            db_columns = csv_columns

            # Convert columns to SQL format
            columns_str = ", ".join(db_columns)
            placeholders = ", ".join(["?"] * len(db_columns))

            # Prepare the insert statement
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # Create a cursor
            cursor = connection.cursor()

            # Insert rows in batches
            batch_size = 1000
            batch = []
            total_inserted = 0  # Tracks the total number of rows inserted

            for row in reader:
                batch.append(row)
                if len(batch) == batch_size:
                    cursor.executemany(insert_sql, batch)
                    connection.commit()
                    total_inserted += len(batch)
                    print(f"Inserted {total_inserted} rows into {table_name}")
                    batch = []

            # Insert any remaining rows
            if batch:
                cursor.executemany(insert_sql, batch)
                connection.commit()
                print(f"Inserted {len(batch)} rows into {table_name}")

        print(f"Data successfully inserted into {table_name}!")
    except Exception as e:
        print(f"Error inserting data into {table_name}:", e)


## funzione per popolare la fact table

def get_primary_key_from_csv(file_path, key_column, matching_column, reference_value):

    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                csv_value = row[matching_column].strip()

                # Confronto standard per colonne non di tipo data
                if csv_value == reference_value:
                    return row[key_column]
        print(f"Valore non trovato: {reference_value} in colonna {matching_column} del file {file_path}")
    except Exception as e:
        print(f"Errore nella lettura del file {file_path}: {e}")
    return None

def get_primary_key_data(file_path, key_column, matching_date_column, matching_time_column, reference_value):

    from datetime import datetime

    try:
        # Separare data e ora dal riferimento
        reference_datetime = datetime.strptime(reference_value, "%m/%d/%Y %I:%M:%S %p")
        reference_date = reference_datetime.date()
        reference_time = reference_datetime.time()

        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Converti data e ora dal CSV
                csv_date = datetime.strptime(row[matching_date_column].strip(), "%Y-%m-%d").date()
                csv_time = datetime.strptime(row[matching_time_column].strip(), "%H:%M:%S").time()

                # Confronta data e ora
                if csv_date == reference_date and csv_time == reference_time:
                    return row[key_column]
        print(f"Valore non trovato: {reference_value} in file {file_path}")
    except ValueError as e:
        print(f"Errore nella conversione di data/ora. Reference: {reference_value} - {e}")
    except Exception as e:
        print(f"Errore nella lettura del file {file_path}: {e}")
    return None


def populate_fact_table(connection):
    csv_directory = "C:/Users/al797/Documents/GitHub/LDS-project-24-25"

    file_paths = {
        "merged": os.path.join(csv_directory, "merged.csv"),
        "crash": os.path.join(csv_directory, "Crash.csv"),
        "road_condition": os.path.join(csv_directory, "RoadCondition.csv"),
        "crash_date": os.path.join(csv_directory, "Crash_date.csv"),
        "cause": os.path.join(csv_directory, "Cause.csv"),
        "geography": os.path.join(csv_directory, "Geography.csv"),
    }

    try:
        with open(file_paths["merged"], mode="r", encoding="utf-8") as merged_file:
            reader = csv.DictReader(merged_file)
            cursor = connection.cursor()
            insert_sql = """ 
                INSERT INTO FACT_DAMAGE (
                    DAMAGE, NUM_UNITS, CRASH_UNIT_ID_FK, PERSON_ID_FK, 
                    CRASH_FK, ROAD_CONDITION_PK_FK, DATE_PK_FK, 
                    CAUSE_PK_FK, GEOGRAPHY_PK_FK
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            batch = []
            batch_size = 1000
            total_inserted = 0

            for row in reader:
                # Prendi i dati principali
                damage = row["DAMAGE"]
                num_units = row["NUM_UNITS"]
                crash_unit_id_fk = row["CRASH_UNIT_ID"]
                person_id_fk = row["PERSON_ID"]

                # Ottieni le chiavi esterne
                road_condition_pk = get_primary_key_from_csv(file_paths["road_condition"], "ROAD_CONDITION_PK", "TRAFFIC_CONTROL_DEVICE", row["TRAFFIC_CONTROL_DEVICE"])
                cause_pk = get_primary_key_from_csv(file_paths["cause"], "CAUSE_PK", "PRIM_CONTRIBUTORY_CAUSE", row["PRIM_CONTRIBUTORY_CAUSE"])
                geography_pk = get_primary_key_from_csv(file_paths["geography"], "GEOGRAPHY_PK", "LOCATION", row["LOCATION"])
                date_pk = get_primary_key_data(
                    file_path=file_paths["crash_date"],
                    key_column="DATE_PK",
                    matching_date_column="CRASH_DATE",
                    matching_time_column="CRASH_HOUR",
                    reference_value=row["CRASH_DATE"]  # Questo valore deve contenere sia data che ora
                )

                crash_pk = get_primary_key_from_csv(
                    file_path=file_paths["crash"],
                    key_column="CRASH_PK",
                    matching_column="RD_NO",
                    reference_value=row["RD_NO"]
                )

                batch.append((damage, num_units, crash_unit_id_fk, person_id_fk, crash_pk, road_condition_pk, date_pk, cause_pk, geography_pk))

                if len(batch) == batch_size:
                    cursor.executemany(insert_sql, batch)
                    connection.commit()
                    total_inserted += len(batch)
                    print(f"Inserted {total_inserted} rows into FACT_DAMAGE")
                    batch = []

            if batch:
                cursor.executemany(insert_sql, batch)
                connection.commit()
                total_inserted += len(batch)
                print(f"Inserted {total_inserted} rows into FACT_DAMAGE")

        print("Data successfully inserted into FACT_DAMAGE!")

    except Exception as e:
        print(f"Error inserting data into FACT_DAMAGE: {e}")
