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


column_mapping = {
    'DAMAGE': 'DAMAGE',
    'NUM_UNITS': 'NUM_UNITS',
    'CRASH_UNIT_ID': 'CRASH_UNIT_ID_FK',
    'CAUSE_PK': 'CAUSE_PK_FK',
    'CRASH_PK': 'CRASH_FK',
    'ROAD_CONDITION_PK': 'ROAD_CONDITION_PK_FK',
    'DATE_PK': 'DATE_PK_FK',
    'PERSON_ID': 'PERSON_ID_FK',
    'GEOGRAPHY_PK': 'GEOGRAPHY_PK_FK'
}
def populate_database_with_mapping(file_path, table_name, connection, column_mapping):
    try:
        # Aprire il file CSV
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)

            # Leggere l'intestazione e trasformarla in base al mapping
            csv_columns = next(reader)  # Legge l'intestazione
            db_columns = [column_mapping[col] for col in csv_columns if col in column_mapping]

            # Convertire colonne in formato SQL
            columns_str = ", ".join(db_columns)
            placeholders = ", ".join(["?"] * len(db_columns))

            # Preparare la query di inserimento
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # Creare un cursore
            cursor = connection.cursor()

            # Inserire i dati
            batch_size = 1000
            batch = []
            total_inserted = 0  # Per tracciare il numero totale di righe inserite

            for row in reader:
                # Applicare il mapping ai dati
                mapped_row = [row[csv_columns.index(col)] for col in csv_columns if col in column_mapping]
                batch.append(mapped_row)
                if len(batch) == batch_size:
                    cursor.executemany(insert_sql, batch)
                    connection.commit()
                    total_inserted += len(batch)
                    print(f"Inserite {total_inserted} righe nella tabella {table_name}")
                    batch = []

            # Inserire le righe rimanenti
            if batch:
                cursor.executemany(insert_sql, batch)
                connection.commit()
                print(f"Inserite {len(batch)} righe rimanenti nella tabella {table_name}")

        print(f"Dati inseriti con successo nella tabella {table_name}!")
    except Exception as e:
        print(f"Errore durante l'inserimento dei dati nella tabella {table_name}:", e)