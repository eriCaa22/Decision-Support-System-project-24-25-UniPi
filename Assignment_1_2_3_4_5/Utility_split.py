from datetime import datetime
import csv

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
    seen_rows = set()  # for duplicated rows

    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        valid_columns = [] # to keep
        for col in columns_to_keep:
            if col in reader.fieldnames:
                valid_columns.append(col)

        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        writer = csv.DictWriter(output_csv, fieldnames=valid_columns)
        writer.writeheader()

        for row in reader:
            filtered_row = {}
            for col in valid_columns:
                filtered_row[col] = row.get(col, '')

            row_identifier = []
            for col in valid_columns:
                if col in filtered_row:
                    row_identifier.append(filtered_row[col])
            row_identifier = tuple(row_identifier)

            if row_identifier in seen_rows: #check if the row has been seen before
                continue

            seen_rows.add(row_identifier)

            writer.writerow(filtered_row)


def create_table_pk(input_file, output_file, columns_to_keep, pk_column_name="ID"):

    try:
        seen_rows = set()
        pk_value = 1

        with open(input_file, mode='r', encoding='utf-8') as input_csv, \
                open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
            reader = csv.DictReader(input_csv)

            valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]
            if not valid_columns:
                raise ValueError(f"Nessuna colonna valida trovata nel file: {input_file}")

            fieldnames = [pk_column_name] + valid_columns
            writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                filtered_row = {col: row[col] for col in valid_columns if col in row}

                row_identifier = tuple(filtered_row.values())
                if row_identifier in seen_rows:
                    continue  # skip duplicates
                seen_rows.add(row_identifier)

                filtered_row[pk_column_name] = pk_value # add pk column
                pk_value += 1

                writer.writerow(filtered_row)

        print(f"Dimension CSV creato con successo: {output_file}. Righe totali: {pk_value - 1}")

    except Exception as e:
        print(f"Errore nella creazione del CSV per {output_file}: {e}")



def create_csv_for_data(input_file, output_file, columns_to_keep, pk_column_name):
    seen_combinations = set()
    data_rows = []
    pk_value = 1

    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]
        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        fieldnames = [pk_column_name] + [
            'CRASH_DATE', 'CRASH_HOUR',
            'DAY', 'MONTH', 'YEAR', 'DAY_OF_WEEK', 'QUARTER', 'DATE_POLICE_NOTIFIED'
        ]
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            try:
                # converting crash date
                crash_date_time = row.get('CRASH_DATE')  # is "2015-09-01 17:00:00"
                if not crash_date_time:
                    raise ValueError("CRASH_DATE mancante")

                date = datetime.strptime(crash_date_time, '%m/%d/%Y %I:%M:%S %p')
                date_text = date.strftime('%Y-%m-%d')

                # we take the hour as INT
                crash_hour_24 = int(date.strftime('%H'))

                # converting DATE_POLICE_NOTIFIED
                date_police_not = row.get('DATE_POLICE_NOTIFIED')
                if date_police_not:
                    date_pol = datetime.strptime(date_police_not, '%m/%d/%Y %I:%M:%S %p')
                    date_text_pol = date_pol.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    date_text_pol = None  # Gestione dei valori mancanti

                combination_key = (
                    date_text,
                    crash_hour_24,
                    date.day,
                    date.month,
                    date.year,
                    date.strftime('%A'),
                    ((date.month - 1) // 3 + 1),
                    date_text_pol
                ) #those are the columns to check ofr duplicates

                if combination_key in seen_combinations:
                    continue #skip
                seen_combinations.add(combination_key)

                new_row = {
                    pk_column_name: pk_value,
                    'CRASH_DATE': date_text,
                    'CRASH_HOUR': crash_hour_24,
                    'DAY': date.day,
                    'MONTH': date.month,
                    'YEAR': date.year,
                    'DAY_OF_WEEK': date.strftime('%A'),
                    'QUARTER': ((date.month - 1) // 3 + 1),
                    'DATE_POLICE_NOTIFIED': date_text_pol,
                } #generating the row with actual values never seen before

                data_rows.append(new_row) #add that row
                pk_value += 1  #

            except Exception as e:
                print(f"Errore nell'elaborazione della riga: {row}. Errore: {e}")
                continue

        writer.writerows(data_rows)
        print(f"CSV con chiavi primarie creato con successo: {output_file}. Righe totali: {pk_value - 1}")


def normalize_date(date_str, input_format, output_format='%Y-%m-%d'):
    try:
        return datetime.strptime(date_str, input_format).strftime(output_format)
    except ValueError:
        return None

def extract_hour(hour_str):
    try:
        return int(hour_str.split(':')[0])
    except (ValueError, AttributeError):
        return None

def normalize_datetime(datetime_str, input_format, output_format='%Y-%m-%d %H:%M:%S'):
    try:
        return datetime.strptime(datetime_str, input_format).strftime(output_format)
    except ValueError:
        return None

def create_csv_for_damage(merged_file, geography_file, cause_file, crash_file, road_condition_file, date_file,
                          output_file):
    import csv

    geography_data = index_file(geography_file, 'LOCATION')
    cause_data = index_file(cause_file, 'PRIM_CONTRIBUTORY_CAUSE')
    crash_data = index_file(crash_file, 'RD_NO')
    road_condition_data = index_file(road_condition_file, 'TRAFFIC_CONTROL_DEVICE')

    # Indicizza Crash_date.csv
    date_data = {}
    with open(date_file, mode='r', encoding='utf-8') as date_csv:
        reader = csv.DictReader(date_csv)
        for row in reader:
            crash_date = normalize_date(row.get('CRASH_DATE', '').strip(), '%Y-%m-%d', '%Y-%m-%d')
            crash_hour = extract_hour(row.get('CRASH_HOUR', '').strip())
            date_police_notified = normalize_datetime(row.get('DATE_POLICE_NOTIFIED', '').strip(),
                                                      '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S')
            key = (crash_date, crash_hour, date_police_notified)
            date_data[key] = row.get('DATE_PK', 'NOT_FOUND')

    with open(merged_file, mode='r', encoding='utf-8') as merged_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as damage_csv:

        reader = csv.DictReader(merged_csv)
        fieldnames = ['DAMAGE', 'NUM_UNITS', 'CRASH_UNIT_ID', 'CAUSE_PK', 'CRASH_PK',
                      'ROAD_CONDITION_PK', 'DATE_PK', 'PERSON_ID', 'GEOGRAPHY_PK']
        writer = csv.DictWriter(damage_csv, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            try:
                crash_date = normalize_date(row.get('CRASH_DATE', '').strip(), '%m/%d/%Y %I:%M:%S %p', '%Y-%m-%d')
                crash_hour = row.get('CRASH_HOUR', '').strip()
                if ":" in crash_hour:
                    crash_hour = int(crash_hour.split(':')[0])
                elif crash_hour.isdigit():
                    crash_hour = int(crash_hour)
                else:
                    crash_hour = None

                date_police_notified = row.get('DATE_POLICE_NOTIFIED', '').strip()
                if date_police_notified:
                    date_police_notified = normalize_datetime(date_police_notified, '%m/%d/%Y %I:%M:%S %p', '%Y-%m-%d %H:%M:%S')
                else:
                    date_police_notified = None

                key = (crash_date, crash_hour, date_police_notified)

                date_pk = date_data.get(key, 'NOT_FOUND')
                if date_pk == 'NOT_FOUND':
                    print(f"Data non trovata per la chiave: {key}")

                new_row = {
                    'DAMAGE': row.get('DAMAGE', ''),
                    'NUM_UNITS': row.get('NUM_UNITS', ''),
                    'CRASH_UNIT_ID': row.get('CRASH_UNIT_ID', ''),
                    'CAUSE_PK': cause_data.get(row.get('PRIM_CONTRIBUTORY_CAUSE', '').strip(), {}).get('CAUSE_PK', 'NOT_FOUND'),
                    'CRASH_PK': crash_data.get(row.get('RD_NO', '').strip(), {}).get('CRASH_PK', 'NOT_FOUND'),
                    'ROAD_CONDITION_PK': road_condition_data.get(row.get('TRAFFIC_CONTROL_DEVICE', '').strip(), {}).get('ROAD_CONDITION_PK', 'NOT_FOUND'),
                    'DATE_PK': date_pk,
                    'PERSON_ID': row.get('PERSON_ID', ''),
                    'GEOGRAPHY_PK': geography_data.get(row.get('LOCATION', '').strip(), {}).get('GEOGRAPHY_PK', 'NOT_FOUND')
                }

                writer.writerow(new_row)

            except Exception as e:
                print(f"Errore nell'elaborazione della riga: {row}. Errore: {e}")
                continue

    print(f"File {output_file} creato con successo.")
