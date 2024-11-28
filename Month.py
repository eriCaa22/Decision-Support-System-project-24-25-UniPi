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


# Funzione per filtrare le colonne e correggere i dati
def filter_and_correct_columns(input_file, output_file, columns_to_keep, date_column=None, month_column=None):
    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        # Identifica le colonne da mantenere
        valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]
        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        # Aggiunge le colonne per correzione se necessario
        if month_column and month_column not in valid_columns:
            valid_columns.append(month_column)

        writer = csv.DictWriter(output_csv, fieldnames=valid_columns)
        writer.writeheader()

        for row in reader:
            # Correzione della data (se specificata)
            if date_column and month_column:
                try:
                    crash_date = datetime.strptime(row[date_column], '%m/%d/%Y %I:%M:%S %p')  # Converte la data
                    row[date_column] = crash_date.strftime('%Y-%m-%d')  # Mantiene solo la data
                    row[month_column] = crash_date.month  # Estrae il mese
                except (ValueError, KeyError):
                    row[date_column] = ''
                    row[month_column] = ''

            # Filtra solo le colonne valide
            filtered_row = {col: row.get(col, '') for col in valid_columns}
            writer.writerow(filtered_row)


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
filter_and_correct_columns('merged.csv', 'Vehicle.csv', col_to_keep_vehicle)
filter_and_correct_columns('merged.csv', 'Crash.csv', col_to_keep_crashes)
filter_and_correct_columns('merged.csv', 'Person.csv', col_to_keep_person)
filter_and_correct_columns('merged.csv', 'Geography.csv', col_to_keep_geography)
filter_and_correct_columns('merged.csv', 'CrashDate.csv', col_to_keep_date, date_column='CRASH_DATE',
                           month_column='CRASH_MONTH')

filter_and_correct_columns('merged.csv', 'Cause.csv', col_to_keep_cause)
filter_and_correct_columns('merged.csv', 'RoadCondition.csv', col_to_keep_roadcond)
filter_and_correct_columns('merged.csv', 'Damage.csv', col_to_keep_damage)
