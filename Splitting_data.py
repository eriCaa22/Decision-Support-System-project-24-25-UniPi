import csv


# Funzione per leggere i file e indicizzarli
def indicizza_file(filepath, key_col):
    indicizzati = {}
    with open(filepath, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = row[key_col]
            indicizzati[key] = row
    return indicizzati


# Funzione per creare il dataset finale
def merge_files(people_file, vehicles_file, crashes_file, key_col, output_file):
    # Indicizziamo i file Vehicles e Crashes
    vehicles_data = indicizza_file(vehicles_file, key_col)
    crashes_data = indicizza_file(crashes_file, key_col)

    # Apriamo il file People e scriviamo il dataset finale
    with open(people_file, mode='r', encoding='utf-8') as people, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output:
        reader = csv.DictReader(people)

        # Ottieni le chiavi delle colonne dei dati Vehicles e Crashes
        vehicles_fieldnames = list(vehicles_data[next(iter(vehicles_data))].keys())
        crashes_fieldnames = list(crashes_data[next(iter(crashes_data))].keys())

        # Combina le fieldnames esistenti con quelle di Vehicles e Crashes
        fieldnames = reader.fieldnames + vehicles_fieldnames + crashes_fieldnames

        # Rimuovere colonne duplicate causate dal merge: trasforma in un dizionario e poi riconverte in lista. Questo
        # perche i dizionari non possono avere duplicati.
        fieldnames = list(dict.fromkeys(fieldnames))

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rd_no = row[key_col]
            # Aggiungiamo i dati da Vehicles e Crashes se disponibili
            vehicle_info = vehicles_data.get(rd_no, {})
            crash_info = crashes_data.get(rd_no, {})

            # Combiniamo i dati in un'unica riga
            merged_row = row.copy()  # Creiamo una copia di row per evitare di modificarlo in-place
            merged_row.update(vehicle_info)  # Aggiungiamo i dati da vehicle_info
            merged_row.update(crash_info)  # Aggiungiamo i dati da crash_info
            writer.writerow(merged_row)


from jupyterlab.semver import valid


def filter_columns(input_file, output_file, columns_to_keep):
    """
    Filtra il dataset per includere solo le colonne specificate.

    :param input_file: Percorso del file sorgente.
    :param output_file: Percorso del file di output filtrato.
    :param columns_to_keep: Elenco delle colonne da includere.
    """
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


merge_files('LDS24-Data/People.csv', 'LDS24-Data/Vehicles.csv', 'LDS24-Data/Crashes.csv', 'RD_NO', output_file)


# Colonne da mantenere
col_to_keep_vehicle = ['CRASH_UNIT_ID_PK', 'VEHICLE_ID', 'UNIT_TYPE', 'UNIT_NO', 'MAKE', 'MODEL', 'VEHCILE_YEAR', 'VEHICLE_DEFECT', 'VEHICLE_USE', 'OCCUPANT_CNT', 'LIC_PLATE_STATE']
col_to_keep_geography = ['GEOGRAPHY_PK', 'STREET_NO', 'STREET_DIRECTION', 'STREET_NAME', 'LATITUDE', 'LONGITUDE', 'LOCATION']
col_to_keep_crashes = ['RD_NO', 'FIRST_CONTACT_POINT', 'FIRST_CRASH_TYPE', 'REPORT_TYPE', 'CRASH_TYPE', 'AIRBAG_DEPLOYED', 'MANEUVER', 'TRAVEL_DIRECTION', 'EJECTION', 'INJURY_PK', 'INJURIES_TOTAL', 'INJURIES_INCAPACITATING', 'INJURIES_NON_INCAPACITATING', 'INJURIES_REPORTED_NOT_EVIDENT', 'INJURIES_NON_INDICATION', 'INJURIES_UNKNOWN', 'MOST_SEVERE_INJURIES']
col_to_keep_date = ['CRASH_DATE_WOT', 'CRASH_HOUR', 'CRASH_DAY_OF_THE_WEEK', 'CRASH_MONTH', 'DATE_POLICE_NOTIFIED']
col_to_keep_person = ['PERSON_ID_PK', 'PERSON_TYPE', 'SEX', 'AGE', 'SAFETY_EQUIPMENT', 'INJURY_CLASSIFICATION', 'CITY']
col_to_keep_cause = ['CAUSE_PK', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE', 'DRIVER_VISION', 'DRIVER_ACTION', 'PHYSICAL_CONDITION', 'BAC_RESULT']
col_to_keep_roadcond = ['ROAD_CONDITION_PK', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'ROADWAY_SURFACE', 'ROAD_DEFECT', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'POSTED_SPEED_LIMIT', 'WEATHER_CONDITION']
col_to_keep_damage = ['DAMAGE_PK', 'DAMAGE', 'NUM_UNITS' ]

# Filtra il dataset e salva in un nuovo file
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_vehicle)
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_geography)
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_crashes)
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_date)
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_person)
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_cause)
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_roadcond)
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep_damage)




















