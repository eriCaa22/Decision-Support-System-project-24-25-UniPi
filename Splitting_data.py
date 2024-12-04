from Utility_split import merge_files, create_csv_for_table, create_csv_for_data, create_table_pk, create_csv_for_damage


# merge to simplify the split
merge_files('People_filled.csv', 'Vehicles_filled.csv', 'Crashes_filled.csv', 'RD_NO', 'merged.csv')

# dict to map the files
files_to_process = {
    'Vehicle.csv': ('create_csv_for_table', ['CRASH_UNIT_ID', 'VEHICLE_ID', 'UNIT_TYPE', 'UNIT_NO', 'MAKE', 'MODEL',
                                            'VEHICLE_YEAR', 'VEHICLE_DEFECT', 'VEHICLE_USE', 'OCCUPANT_CNT', 'VEHICLE_TYPE'
                                            'LIC_PLATE_STATE'], ''),
    'Crash.csv': ('create_table_pk', ['RD_NO', 'FIRST_CONTACT_POINT', 'FIRST_CRASH_TYPE', 'REPORT_TYPE',
                                           'CRASH_TYPE', 'AIRBAG_DEPLOYED', 'MANEUVER', 'TRAVEL_DIRECTION',
                                           'EJECTION', 'INJURIES_TOTAL', 'INJURIES_INCAPACITATING',
                                          'INJURIES_NON_INCAPACITATING', 'INJURIES_REPORTED_NOT_EVIDENT',
                                           'INJURIES_NON_INDICATION', 'INJURIES_UNKNOWN',
                                           'MOST_SEVERE_INJURIES'], 'CRASH_PK'),
    'Person.csv': ('create_csv_for_table', ['PERSON_ID', 'PERSON_TYPE', 'SEX', 'AGE', 'SAFETY_EQUIPMENT',
                                            'INJURY_CLASSIFICATION', 'CITY'], ''),
    'Geography.csv': ('create_table_pk', ['STREET_NO', 'STREET_DIRECTION', 'STREET_NAME',
                                               'LATITUDE', 'LONGITUDE', 'LOCATION'], 'GEOGRAPHY_PK'),
    'Cause.csv': ('create_table_pk', ['PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE',
                                           'DRIVER_VISION', 'DRIVER_ACTION', 'PHYSICAL_CONDITION', 'BAC_RESULT'], 'CAUSE_PK'),
    'RoadCondition.csv': ('create_table_pk', [ 'TRAFFIC_CONTROL_DEVICE',
                                                   'DEVICE_CONDITION', 'ROADWAY_SURFACE_COND', 'ROAD_DEFECT',
                                                   'TRAFFICWAY_TYPE', 'ALIGNMENT', 'POSTED_SPEED_LIMIT',
                                                   'WEATHER_CONDITION'], 'ROAD_CONDITION_PK'),
    'Damage_2.csv': ('create_csv_for_damage', ['DAMAGE', 'NUM_UNITS', 'CRASH_UNIT_ID', 'CAUSE_PK', 'CRASH_PK',
                                            'ROAD_CONDITION_PK', 'DATE_PK', 'PERSON_ID', 'GEOGRAPHY_PK'], ''),
    'Crash_date_2.csv': ('create_csv_for_data', ['CRASH_DATE', 'CRASH_HOUR', 'DAY', 'MONTH', 'YEAR', 'DAY_OF_WEEK',
                                                 'QUARTER', 'DATE_POLICE_NOTIFIED'], "DATE_PK")
}


for file_name, (function_name, columns, pk) in files_to_process.items():
    if function_name == 'create_csv_for_table':
        create_csv_for_table('merged.csv', file_name, columns)
    if function_name == 'create_csv_for_data':
        create_csv_for_data('merged.csv', file_name, columns, pk)
    if function_name == 'create_table_pk':
        create_table_pk('merged.csv', file_name, columns, pk)
    elif function_name == 'create_csv_for_damage':
        create_csv_for_damage('merged.csv','Geography.csv', 'Cause.csv', 'Crash.csv',
                              'RoadCondition.csv', 'Crash_date.csv', 'Damage_2.csv')


