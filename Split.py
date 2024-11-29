from Utility import merge_files, create_csv_for_table, create_csv_for_data


# File di input e output
merge_files('People_filled.csv', 'Vehicles_filled.csv', 'Crashes_filled.csv', 'RD_NO', 'merged.csv')

# Dizionario per mappare i file con la funzione appropriata e le colonne
files_to_process = {
    'Vehicle.csv': ('create_csv_for_table', ['CRASH_UNIT_ID', 'VEHICLE_ID', 'UNIT_TYPE', 'UNIT_NO', 'MAKE', 'MODEL',
                                             'VEHICLE_YEAR', 'VEHICLE_DEFECT', 'VEHICLE_USE', 'OCCUPANT_CNT',
                                             'LIC_PLATE_STATE']),
    'Crash.csv': ('create_csv_for_table', ['RD_NO', 'FIRST_CONTACT_POINT', 'FIRST_CRASH_TYPE', 'REPORT_TYPE',
                                           'CRASH_TYPE', 'AIRBAG_DEPLOYED', 'MANEUVER', 'TRAVEL_DIRECTION',
                                           'EJECTION', 'INJURIES_TOTAL', 'INJURIES_INCAPACITATING',
                                           'INJURIES_NON_INCAPACITATING', 'INJURIES_REPORTED_NOT_EVIDENT',
                                           'INJURIES_NON_INDICATION', 'INJURIES_UNKNOWN',
                                           'MOST_SEVERE_INJURIES']),
    'Person.csv': ('create_csv_for_table', ['PERSON_ID', 'PERSON_TYPE', 'SEX', 'AGE', 'SAFETY_EQUIPMENT',
                                            'INJURY_CLASSIFICATION', 'CITY']),
    'Geography.csv': ('create_csv_for_table', ['GEOGRAPHY_PK', 'STREET_NO', 'STREET_DIRECTION', 'STREET_NAME',
                                               'LATITUDE', 'LONGITUDE', 'LOCATION']),
    'Cause.csv': ('create_csv_for_table', ['CAUSE_PK', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE',
                                           'DRIVER_VISION', 'DRIVER_ACTION', 'PHYSICAL_CONDITION', 'BAC_RESULT']),
    'RoadCondition.csv': ('create_csv_for_table', ['ROAD_CONDITION_PK', 'TRAFFIC_CONTROL_DEVICE',
                                                   'DEVICE_CONDITION', 'ROADWAY_SURFACE_COND', 'ROAD_DEFECT',
                                                   'TRAFFICWAY_TYPE', 'ALIGNMENT', 'POSTED_SPEED_LIMIT',
                                                   'WEATHER_CONDITION']),
    'Damage.csv': ('create_csv_for_table', ['DAMAGE', 'NUM_UNITS', 'CRASH_UNIT_ID', 'CAUSE_PK', 'CRASH_PK',
                                            'ROAD_CONDITION_PK', 'DATE_PK', 'CAUSE_PK', 'PERSON_ID']),
    'Crash_date.csv': ('create_csv_for_data', ['CRASH_DATE', 'CRASH_HOUR', 'DAY', 'MONTH', 'YEAR', 'DAY_OF_WEEK', 'QUARTER', 'DATE_POLICE_NOTIFIED'])
}

# Processa i file
for file_name, (function_name, columns) in files_to_process.items():
    if function_name == 'create_csv_for_table':
        create_csv_for_table('merged.csv', file_name, columns)
    elif function_name == 'create_csv_for_data':
        create_csv_for_data('merged.csv', file_name, columns)


