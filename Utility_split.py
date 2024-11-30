import csv
from datetime import datetime

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


import csv


import csv

def create_table_pk(input_file, output_file, columns_to_keep, pk_column_name="ID"):

    try:
        seen_rows = set()  # Per tracciare i duplicati
        pk_value = 1  # Valore iniziale della chiave primaria

        with open(input_file, mode='r', encoding='utf-8') as input_csv, \
                open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
            reader = csv.DictReader(input_csv)

            # Verifica colonne valide
            valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]
            if not valid_columns:
                raise ValueError(f"Nessuna colonna valida trovata nel file: {input_file}")

            # Configura il writer con la chiave primaria e le colonne valide
            fieldnames = [pk_column_name] + valid_columns
            writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
            writer.writeheader()

            # Iterazione sulle righe del file di input
            for row in reader:
                # Filtra solo le colonne valide
                filtered_row = {col: row[col] for col in valid_columns if col in row}

                # Genera una chiave unica per identificare duplicati
                row_identifier = tuple(filtered_row.values())
                if row_identifier in seen_rows:
                    continue  # Salta i duplicati
                seen_rows.add(row_identifier)

                # Aggiungi la chiave primaria incrementale
                filtered_row[pk_column_name] = pk_value
                pk_value += 1

                # Scrivi la riga nel file di output
                writer.writerow(filtered_row)

        print(f"Dimension CSV creato con successo: {output_file}. Righe totali: {pk_value - 1}")

    except Exception as e:
        print(f"Errore nella creazione del CSV per {output_file}: {e}")



def create_csv_for_data(input_file, output_file, columns_to_keep, pk_column_name):

    # Lista per salvare le righe elaborate
    data_rows = []
    seen_rows = set()  # Set per tracciare i duplicati
    pk_value = 1  # Valore iniziale per la chiave primaria

    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        # Identifica le colonne valide
        valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]
        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        # Configura il writer con la chiave primaria e le altre colonne
        fieldnames = [pk_column_name] + [
            'CRASH_DATE', 'CRASH_HOUR',
            'DAY', 'MONTH', 'YEAR', 'DAY_OF_WEEK', 'QUARTER', 'DATE_POLICE_NOTIFIED'
        ]
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()

        # Iterazione sulle righe del file CSV
        for row in reader:
            try:
                # Filtra solo le colonne richieste
                filtered_row = {col: row[col] for col in valid_columns if col in row}

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
                    pk_column_name: pk_value,  # Chiave primaria progressiva
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
                row_identifier = tuple(list(new_row.values())[1:])  # Converte dict_values in lista prima dello slicing
                if row_identifier in seen_rows:
                    continue  # Salta i duplicati
                seen_rows.add(row_identifier)  # Aggiungi la riga al set dei duplicati
                data_rows.append(new_row)
                pk_value += 1  # Incrementa la chiave primaria

            except Exception as e:
                # Gestione di eventuali errori di parsing della data
                print(f"Errore nell'elaborazione della riga: {row}. Errore: {e}")
                continue

        # Scrive le righe elaborate nel file di output
        writer.writerows(data_rows)
        print(f"CSV con chiavi primarie creato con successo: {output_file}. Righe totali: {pk_value - 1}")
