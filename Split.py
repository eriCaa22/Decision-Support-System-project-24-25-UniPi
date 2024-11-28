import csv
from datetime import datetime


# Funzione per leggere i file e indicizzarli
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
        valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]
        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        writer = csv.DictWriter(output_csv, fieldnames=valid_columns)
        writer.writeheader()

        for row in reader:

            # Filtra solo le colonne valide
            filtered_row = {col: row.get(col, '') for col in valid_columns}

            # Genera un identificatore univoco per la riga (basato sui valori delle colonne)
            row_identifier = tuple(filtered_row[col] for col in valid_columns if col in filtered_row)

            # Salta i duplicati
            if row_identifier in seen_rows:
                continue

            # Aggiungi la riga al set di righe viste e scrivila nel file di output
            seen_rows.add(row_identifier)
            writer.writerow(filtered_row)

def create_csv_for_data(input_file, output_file, columns_to_keep):
    # Lista per salvare le righe elaborate
    data_rows = []

    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        # Identifica le colonne da mantenere che esistono nel dataset
        valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]
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
                filtered_row = {col: row[col] for col in valid_columns}

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
                data_rows.append({
                    'CRASH_DATE': date_text,
                    'CRASH_HOUR': crash_hour_24,  # Mantiene l'ora originale
                    'DAY': day,
                    'MONTH': month,
                    'YEAR': year,
                    'DAY_OF_WEEK': day_of_week,
                    'QUARTER': quarter_of_year,
                    'DATE_POLICE_NOTIFIED': date_text_pol,
                })

            except Exception as e:
                # Gestione di eventuali errori di parsing della data
                print(f"Errore nell'elaborazione della riga: {row}. Errore: {e}")
                continue

        # Scrive le righe elaborate nel file di output
        writer.writerows(data_rows)

# File di input e output
merge_files('People_filled.csv', 'Vehicles_filled.csv', 'Crashes_filled.csv', 'RD_NO', 'merged.csv')

# Definizione delle colonne per ciascun file
col_to_keep_vehicle = ['CRASH_UNIT_ID', 'VEHICLE_ID', 'UNIT_TYPE', 'UNIT_NO', 'MAKE', 'MODEL', 'VEHICLE_YEAR',
                       'VEHICLE_DEFECT', 'VEHICLE_USE', 'OCCUPANT_CNT', 'LIC_PLATE_STATE']
col_to_keep_geography = ['GEOGRAPHY_PK', 'STREET_NO', 'STREET_DIRECTION', 'STREET_NAME', 'LATITUDE', 'LONGITUDE',
                         'LOCATION']
col_to_keep_crashes = ['RD_NO', 'FIRST_CONTACT_POINT', 'FIRST_CRASH_TYPE', 'REPORT_TYPE', 'CRASH_TYPE',
                       'AIRBAG_DEPLOYED', 'MANEUVER', 'TRAVEL_DIRECTION', 'EJECTION', 'INJURIES_TOTAL',
                       'INJURIES_INCAPACITATING', 'INJURIES_NON_INCAPACITATING', 'INJURIES_REPORTED_NOT_EVIDENT',
                       'INJURIES_NON_INDICATION', 'INJURIES_UNKNOWN', 'MOST_SEVERE_INJURIES']
col_to_keep_date = ['DATE_PK', 'CRASH_DATE', 'CRASH_HOUR', 'CRASH_DAY_OF_WEEK', 'CRASH_MONTH',
                    'DATE_POLICE_NOTIFIED']
col_to_keep_person = ['PERSON_ID', 'PERSON_TYPE', 'SEX', 'AGE', 'SAFETY_EQUIPMENT', 'INJURY_CLASSIFICATION', 'CITY']
col_to_keep_cause = ['CAUSE_PK', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE', 'DRIVER_VISION', 'DRIVER_ACTION',
                     'PHYSICAL_CONDITION', 'BAC_RESULT']
col_to_keep_roadcond = ['ROAD_CONDITION_PK', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'ROADWAY_SURFACE_COND',
                        'ROAD_DEFECT', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'POSTED_SPEED_LIMIT', 'WEATHER_CONDITION']
col_to_keep_damage = ['DAMAGE_PK', 'DAMAGE', 'NUM_UNITS']


# Filtra e corregge i dati
create_csv_for_table('merged.csv', 'Vehicle.csv', col_to_keep_vehicle)
create_csv_for_table('merged.csv', 'Crash.csv', col_to_keep_crashes)
create_csv_for_table('merged.csv', 'Person.csv', col_to_keep_person)
create_csv_for_table('merged.csv', 'Geography.csv', col_to_keep_geography)
create_csv_for_table('merged.csv', 'Cause.csv', col_to_keep_cause)
create_csv_for_table('merged.csv', 'RoadCondition.csv', col_to_keep_roadcond)
create_csv_for_table('merged.csv', 'Damage.csv', col_to_keep_damage)

create_csv_for_data('merged.csv', 'CrashDate.csv', col_to_keep_date)
