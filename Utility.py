import csv
from datetime import datetime
import pyodbc

### FUNZIONI PER SPLIT

def index_file(filepath, key_column):
    indexed_data = {}
    with open(filepath, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = row[key_column]
            indexed_data[key] = row
    return indexed_data

# Funzione per creare il dataset finale
def merge_files(people_file, vehicles_file, crashes_file, key_col, output_file):
    vehicles_data = index_file(vehicles_file, key_col)
    crashes_data = index_file(crashes_file, key_col)

    with open(people_file, mode='r', encoding='utf-8') as people, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output:
        reader = csv.DictReader(people)
        fieldnames = reader.fieldnames + list(vehicles_data[next(iter(vehicles_data))].keys()) + \
                     list(crashes_data[next(iter(crashes_data))].keys())

        fieldnames = list(dict.fromkeys(fieldnames))  # Rimuove eventuali duplicati
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rd_no = row[key_col]
            vehicle_info = vehicles_data.get(rd_no, {})
            crash_info = crashes_data.get(rd_no, {})
            merged_row = {**row, **vehicle_info, **crash_info}
            writer.writerow(merged_row)


# Funzione per filtrare le colonne, correggere i dati e rimuovere duplicati
def create_csv_for_table(input_file, output_file, columns_to_keep, date_column=None, month_column=None):
    seen_rows = set()  # Per tracciare i duplicati

    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        # Identifica le colonne da mantenere

        valid_columns = []
        for col in columns_to_keep:
            if col in reader.fieldnames:
                valid_columns.append(col)

        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        writer = csv.DictWriter(output_csv, fieldnames=valid_columns)
        writer.writeheader()

        for row in reader:
            # Inizializza un dizionario per la riga filtrata
            filtered_row = {}
            for col in valid_columns:
                # Aggiungi al dizionario il valore della colonna o una stringa vuota se non presente
                filtered_row[col] = row.get(col, '')

            # Genera l'identificatore univoco come tupla
            row_identifier = []
            for col in valid_columns:
                if col in filtered_row:
                    row_identifier.append(filtered_row[col])
            row_identifier = tuple(row_identifier)

            # Controlla se l'identificatore è già stato visto
            if row_identifier in seen_rows:
                continue

            # Aggiungi l'identificatore al set di quelli visti
            seen_rows.add(row_identifier)

            # Scrivi la riga filtrata nel writer
            writer.writerow(filtered_row)


def create_csv_for_data(input_file, output_file, columns_to_keep):
    # Lista per salvare le righe elaborate
    data_rows = []
    seen_rows = set()  # Set per tracciare le righe già viste

    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        # Identifica le colonne da mantenere che esistono nel dataset

        valid_columns = []
        for col in columns_to_keep:
            if col in reader.fieldnames:
                valid_columns.append(col)

        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        # Configura il writer con le colonne aggiunte
        fieldnames = [
            'CRASH_DATE', 'CRASH_HOUR',
            'DAY', 'MONTH', 'YEAR', 'DAY_OF_WEEK', 'QUARTER',  'DATE_POLICE_NOTIFIED'
        ]
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()

        # Iterazione su ogni riga del file CSV
        for row in reader:
            try:
                # Filtra le colonne richieste
                filtered_row = {}
                for col in valid_columns:
                    if col in row:  # Verifica se la colonna esiste nella riga
                        filtered_row[col] = row[col]

                # Estrazione e conversione del campo CRASH_DATE
                crash_date_time = filtered_row.get('CRASH_DATE')  # Es. "2015-09-01 17:00:00"
                date_police_not = filtered_row.get('DATE_POLICE_NOTIFIED')  # Es. "09/01/2015 06:45:00 PM"

                # Conversione delle stringhe in oggetti datetime
                date = datetime.strptime(crash_date_time, '%m/%d/%Y %I:%M:%S %p')
                date_pol = datetime.strptime(date_police_not, '%m/%d/%Y %I:%M:%S %p')  # Gestisce AM/PM

                # Estrazione delle informazioni richieste per crash_date
                date_text = date.strftime('%Y-%m-%d')  # Solo la data in formato yyyy-mm-dd
                crash_hour_24 = date.strftime('%H:%M:%S')
                year = date.year
                month = date.month
                day = date.day
                day_of_week = date.strftime('%A')  # Giorno della settimana (es. "Thursday")
                quarter_of_year = ((month - 1) // 3 + 1)  # Calcolo del trimestre

                # Conversione di date_police_notified in formato a 24 ore
                date_text_pol = date_pol.strftime('%Y-%m-%d %H:%M:%S')  # Formato 24 ore

                # Creazione di una riga con i dati elaborati
                new_row = {
                    'CRASH_DATE': date_text,
                    'CRASH_HOUR': crash_hour_24,  # Mantiene l'ora originale
                    'DAY': day,
                    'MONTH': month,
                    'YEAR': year,
                    'DAY_OF_WEEK': day_of_week,
                    'QUARTER': quarter_of_year,
                    'DATE_POLICE_NOTIFIED': date_text_pol,
                }

                # Controllo dei duplicati
                row_identifier = tuple(new_row.values())  # Usa i valori per identificare univocamente la riga
                if row_identifier in seen_rows:
                    continue  # Salta la riga se è un duplicato
                seen_rows.add(row_identifier)  # Aggiungi la riga al set dei duplicati
                data_rows.append(new_row)

            except Exception as e:
                # Gestione di eventuali errori di parsing della data
                print(f"Errore nell'elaborazione della riga: {row}. Errore: {e}")
                continue

        # Scrive le righe elaborate nel file di output
        writer.writerows(data_rows)

### FUNZIONI PER IL POPOLAMENTO

# Connect to the database
def connect_to_db():
    SERVER = 'lds.di.unipi.it'
    DATABASE = 'Group_ID_24_DB'
    USERNAME = 'Group_ID_24'
    PASSWORD = 'IMTGP44N'
    DRIVER = 'ODBC Driver 17 for SQL Server'

    try:
        connection = pyodbc.connect(
            f'DRIVER={{{DRIVER}}};'
            f'SERVER={SERVER};'
            f'DATABASE={DATABASE};'
            f'UID={USERNAME};'
            f'PWD={PASSWORD}'
        )
        print("Database connection successful!")
        return connection
    except Exception as e:
        print("Error connecting to database:", e)
        return None

# Read and insert CSV data
def populate_database(file_path, table_name, connection):
    try:
        # Open the CSV file
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)

            # Get the column names from the first row
            columns = next(reader)  # Read the header row
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))

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

