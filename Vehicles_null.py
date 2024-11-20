from Utility import replace_nulls, replace_nulls_with_conditions, assign_random_values, replace_missing_with_value_from_another_df
import csv

# Funzione generica per applicare sostituzioni condizionali a più colonne
def apply_replacements(dataset, replacements_dict):
    for column, conditions in replacements_dict.items():
        for unit_type, replacement_value in conditions.items():
            replace_nulls_with_conditions(dataset, 'UNIT_TYPE', unit_type, column, replacement_value)

# Caricamento del dataset
def load_csv(filepath):
    with open(filepath, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

vehicles = load_csv('LDS24 - Data/Vehicles.csv')

# Sostituzioni condizionali per colonne
replacements = {
    'VEHICLE_ID': {
        'PEDESTRIAN': 0.0,
        'BICYCLE': 0.0,
        'NON-MOTOR VEHICLE': 0.0,
        'NON-CONTACT VEHICLE': 0.1,
        'DRIVER': 0.2,
    },
    'MAKE': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 0.3,
        'DRIVER': 0.4,
    },
    'MODEL': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 0.3,
        'DRIVER': 0.4,
        'PARKED': 0.5,
        'DRIVERLESS': 0.6,
    },
    'LIC_PLATE_STATE': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 0.3,
        'DRIVER': 0.4,
        'PARKED': 0.5,
        'DRIVERLESS': 0.6,
    },
    'VEHICLE_YEAR': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 'UNKNOWN',
        'DRIVER': 'UNKNOWN',
        'PARKED': 'UNKNOWN',
        'DRIVERLESS': 'NON APPLICABLE',
    },
    'VEHICLE_DEFECT': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 'UNKNOWN',
        'DRIVER': 'UNKNOWN',
    },
    'VEHICLE_TYPE': {
        'PEDESTRIAN': 'NON-MOTOR VEHICLE',
        'BICYCLE': 'NON-MOTOR VEHICLE',
        'NON-MOTOR VEHICLE': 'NON-MOTOR VEHICLE',
        'NON-CONTACT VEHICLE': 'UNKNOWN',
        'DRIVER': 'SPORT UTILITY VEHICLE (SUV)',
    },
    'MANEUVER': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 'UNKNOWN',
        'DRIVER': 'UNKNOWN',
    },
    'OCCUPANT_CNT': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 'UNKNOWN',
        'DRIVER': 'UNKNOWN',
    },
    'FIRST_CONTACT_POINT': {
        'PEDESTRIAN': 'NON APPLICABLE',
        'BICYCLE': 'NON APPLICABLE',
        'NON-MOTOR VEHICLE': 'NON APPLICABLE',
        'NON-CONTACT VEHICLE': 'UNKNOWN',
        'DRIVER': 'UNKNOWN',
    },
}

# Applicare le sostituzioni condizionali
apply_replacements(vehicles, replacements)

# Altre trasformazioni
replace_nulls(vehicles, 'UNIT_TYPE', 'BICYCLE')
assign_random_values(vehicles, 'VEHICLE_USE', ['PERSONAL', 'UNKNOWN'], [0.67, 0.33])

# Caricamento e sincronizzazione con il dataset Crashes
crashes = load_csv('LDS24 - Data/Crashes.csv')
replace_missing_with_value_from_another_df(
    vehicles, crashes, 'RD_NO', 'TRAVEL_DIRECTION', 'STREET_DIRECTION'
)
