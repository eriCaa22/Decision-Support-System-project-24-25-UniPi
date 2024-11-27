#from Utility import merge_files, filter_columns

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

        fieldnames = list(dict.fromkeys(fieldnames))

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rd_no = row[key_col]
            # Aggiungiamo i dati da Vehicles e Crashes se disponibili
            vehicle_info = vehicles_data.get(rd_no, {})
            crash_info = crashes_data.get(rd_no, {})

            # Combiniamo i dati in un'unica riga
            merged_row = {**row, **vehicle_info, **crash_info}
            writer.writerow(merged_row)

def filter_columns(input_file, output_file, columns_to_keep):
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

        # Configura il writer con le colonne filtrate
        writer = csv.DictWriter(output_csv, fieldnames=valid_columns)
        writer.writeheader()

        # Scrive solo le righe con le colonne filtrate
        for row in reader:
            filtered_row = {}
            for col in valid_columns:
                filtered_row[col] = row[col]
            writer.writerow(filtered_row)

import csv

def filter_columns_with_primary_key(input_file, output_file, columns_to_keep, primary_key_name="id"):
    with open(input_file, mode='r', encoding='utf-8') as input_csv, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output_csv:
        reader = csv.DictReader(input_csv)

        valid_columns = [col for col in columns_to_keep if col in reader.fieldnames]

        valid_columns.insert(0, primary_key_name)

        if not valid_columns:
            raise ValueError("Nessuna delle colonne specificate è presente nel dataset.")

        writer = csv.DictWriter(output_csv, fieldnames=valid_columns)
        writer.writeheader()

        primary_key_value = 1
        for row in reader:
            filtered_row = {col: row[col] for col in columns_to_keep if col in row}
            filtered_row[primary_key_name] = primary_key_value
            primary_key_value += 1
            writer.writerow(filtered_row)



## CREIAMO IL FILE UNICO - MERGE

merge_files('People_filled.csv', 'Vehicles_filled.csv', 'Crashes_filled.csv', 'RD_NO', 'merged.csv')


# Colonne da mantenere
col_to_keep_vehicle = ['CRASH_UNIT_ID_PK', 'VEHICLE_ID', 'UNIT_TYPE', 'UNIT_NO', 'MAKE', 'MODEL', 'VEHICLE_YEAR', 'VEHICLE_DEFECT', 'VEHICLE_USE', 'OCCUPANT_CNT', 'LIC_PLATE_STATE']
col_to_keep_geography = ['GEOGRAPHY_PK', 'STREET_NO', 'STREET_DIRECTION', 'STREET_NAME', 'LATITUDE', 'LONGITUDE', 'LOCATION']
col_to_keep_crashes = ['RD_NO', 'FIRST_CONTACT_POINT', 'FIRST_CRASH_TYPE', 'REPORT_TYPE', 'CRASH_TYPE', 'AIRBAG_DEPLOYED', 'MANEUVER', 'TRAVEL_DIRECTION', 'EJECTION', 'INJURY_PK', 'INJURIES_TOTAL', 'INJURIES_INCAPACITATING', 'INJURIES_NON_INCAPACITATING', 'INJURIES_REPORTED_NOT_EVIDENT', 'INJURIES_NON_INDICATION', 'INJURIES_UNKNOWN', 'MOST_SEVERE_INJURIES']
col_to_keep_date = ['CRASH_DATE_WOT', 'CRASH_HOUR', 'CRASH_DAY_OF_THE_WEEK', 'CRASH_MONTH', 'DATE_POLICE_NOTIFIED']
col_to_keep_person = ['PERSON_ID_PK', 'PERSON_TYPE', 'SEX', 'AGE', 'SAFETY_EQUIPMENT', 'INJURY_CLASSIFICATION', 'CITY']
col_to_keep_cause = ['CAUSE_PK', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE', 'DRIVER_VISION', 'DRIVER_ACTION', 'PHYSICAL_CONDITION', 'BAC_RESULT']
col_to_keep_roadcond = ['ROAD_CONDITION_PK', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'ROADWAY_SURFACE', 'ROAD_DEFECT', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'POSTED_SPEED_LIMIT', 'WEATHER_CONDITION']
col_to_keep_damage = ['DAMAGE_PK', 'DAMAGE', 'NUM_UNITS' ]

# Filtra il dataset e salva in un nuovo file
filter_columns('merged.csv', 'Vehicle.csv', col_to_keep_vehicle)
filter_columns('merged.csv', 'Crash.csv', col_to_keep_crashes)
filter_columns('merged.csv', 'Person.csv', col_to_keep_person)
filter_columns_with_primary_key('merged.csv', 'Geography.csv', col_to_keep_geography, 'Geography_PK')
filter_columns_with_primary_key('merged.csv', 'CrashDate.csv', col_to_keep_date, 'CrashDate_PK')
filter_columns_with_primary_key('merged.csv', 'Cause.csv', col_to_keep_cause, 'Cause_PK')
filter_columns_with_primary_key('merged.csv', 'RoadCondition.csv', col_to_keep_roadcond, 'RoadCondition_PK')
filter_columns_with_primary_key('merged.csv', 'Damage.csv', col_to_keep_damage, 'Damage_PK')





















