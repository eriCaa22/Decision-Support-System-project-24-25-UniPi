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

import csv
import os
import pyodbc

# Funzione per ottenere la chiave primaria dal CSV corrispondente
def get_primary_key_from_csv(file_path, key_column, matching_value):
    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row[key_column] == matching_value:  # Cambia "id" con la colonna di matching appropriata
                    return row[key_column]
    except Exception as e:
        print(f"Error reading {file_path}:", e)
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

            insert_sql = """ 
                INSERT INTO FACT_DAMAGE (
                    DAMAGE, NUM_UNITS, CRASH_UNIT_ID_FK, PERSON_ID_FK, 
                    CRASH_FK, ROAD_CONDITION_PK_FK, DATE_PK_FK, 
                    CAUSE_PK_FK, GEOGRAPHY_PK_FK
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor = connection.cursor()
            batch = []
            batch_size = 1000
            total_inserted = 0

            for i, row in enumerate(reader):
                if i % 100 == 0:  # Stampa un log ogni 100 righe
                    print(f"Processing row {i}...")

                # Ottieni i dati principali da merged.csv
                damage = row["DAMAGE"]
                num_units = row["NUM_UNITS"]
                crash_unit_id_fk = row["CRASH_UNIT_ID"]
                person_id_fk = row["PERSON_ID"]

                # Ottieni le chiavi esterne dai rispettivi CSV
                crash_pk = get_primary_key_from_csv(file_paths["crash"], "CRASH_PK", row["RD_NO"])
                road_condition_pk = get_primary_key_from_csv(file_paths["road_condition"], "ROAD_CONDITION_PK", row["TRAFFIC_CONTROL_DEVICE"])
                date_pk = get_primary_key_from_csv(file_paths["crash_date"], "DATE_PK", row["CRASH_DATE"])
                cause_pk = get_primary_key_from_csv(file_paths["cause"], "CAUSE_PK", row["PRIM_CONTRIBUTORY_CAUSE"])
                geography_pk = get_primary_key_from_csv(file_paths["geography"], "GEOGRAPHY_PK", row["LOCATION"])

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
