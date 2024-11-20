############################################
# CALL FOR THE FUNCTIONS IN UTILITY
############################################

from Utility import replace_nulls, replace_nulls_with_conditions, assign_random_values, replace_missing_with_value_from_another_df

############################################
# APPLICATION OF THE FUNCTIONS TO THE DATA
############################################



###### VEHICLES DATASET ######

# Load the dataset
with open('LDS24 - Data/Vehicles.csv', mode='r') as file:
    reader = csv.DictReader(file)
    vehicles = [row for row in reader]

# Replace null values in the 'UNIT_TYPE' column
replace_nulls(vehicles, 'UNIT_TYPE', 'BICYCLE')

# Replace null values in the 'VEHICLE_ID' column based on 'UNIT_TYPE' conditions
# We used a dictionary to store the replacement values for each condition
unit_type_vehicle_id_replacements = {
    'PEDESTRIAN': 0.0,
    'BICYCLE': 0.0,
    'NON-MOTOR VEHICLE': 0.0,
    'NON-CONTACT VEHICLE': 0.1,
    'DRIVER': 0.2,
}

for unit_type, replacement_value in unit_type_vehicle_id_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'VEHICLE_ID', replacement_value)

# Replace null values in the 'MAKE' column based on 'UNIT_TYPE' conditions
unit_type_make_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 0.3,
    'DRIVER': 0.4,
}

for unit_type, replacement_value in unit_type_make_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'MAKE', replacement_value)

# Replace null values in the 'MODEL' column based on 'UNIT_TYPE' conditions
unit_type_model_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 0.3,
    'DRIVER': 0.4,
    'PARKED': 0.5,
    'DRIVERLESS': 0.6,
}

for unit_type, replacement_value in unit_type_model_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'MODEL', replacement_value)


# Replace null values in the 'LIC_PLATE_STATE' column based on 'UNIT_TYPE' conditions
unit_type_license_plate_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 0.3,
    'DRIVER': 0.4,
    'PARKED': 0.5,
    'DRIVERLESS': 0.6,
}

for unit_type, replacement_value in unit_type_license_plate_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'LIC_PLATE_STATE', replacement_value)

# Replace null values in the 'VEHICLE_YEAR' column based on 'UNIT_TYPE' conditions
unit_type_vehicle_year_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 'UNKNOWN',
    'DRIVER': 'UNKNOWN',
    'PARKED': 'UNKNOWN',
    'DRIVERLESS': 'NON APPLICABLE',
}

for unit_type, replacement_value in unit_type_vehicle_year_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'VEHICLE_YEAR', replacement_value)

# Replace null values in the 'VEHICLE_DEFECT' column based on 'UNIT_TYPE' conditions
unit_type_vehicle_defect_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 'UNKNOWN',
    'DRIVER': 'UNKNOWN',
}

for unit_type, replacement_value in unit_type_vehicle_defect_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'VEHICLE_DEFECT', replacement_value)


# Replace null values in the 'VEHICLE_TYPE' column based on 'UNIT_TYPE' conditions
unit_type_vehicle_type_replacements = {
    'PEDESTRIAN': 'NON-MOTOR VEHICLE',
    'BICYCLE': 'NON-MOTOR VEHICLE',
    'NON-MOTOR VEHICLE': 'NON-MOTOR VEHICLE',
    'NON-CONTACT VEHICLE': 'UNKNOWN',
    'DRIVER': 'SPORT UTILITY VEHICLE (SUV)',
}

for unit_type, replacement_value in unit_type_vehicle_type_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'VEHICLE_TYPE', replacement_value)

# Using the function assign_random_values to replace missing values in the 'VEHICLE_USE' column
values = ['PERSONAL', 'UNKNOWN']
probabilities = [0.67, 0.33]
assign_random_values(vehicles, 'VEHICLE_USE', values, probabilities)

# To replace the missing values of Travel Direction in the Vehicles dataset with the values from the Crashes dataset

import csv

with open('LDS24 - Data/Crashes.csv', mode='r') as file:
    reader = csv.DictReader(file)
    crashes = [row for row in reader]
#%%
replace_missing_with_value_from_another_df(
    vehicles, crashes, 'RD_NO', 'TRAVEL_DIRECTION', 'STREET_DIRECTION')



# Replace null values in the 'MANEUVER' column based on 'UNIT_TYPE' conditions
unit_type_maneuver_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 'UNKNOWN',
    'DRIVER': 'UNKNOWN',
}

for unit_type, replacement_value in unit_type_maneuver_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'MANEUVER', replacement_value)

# Replace null values in the 'OCCUPANT_CNT' column based on 'UNIT_TYPE' conditions
unit_type_occupant_count_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 'UNKNOWN',
    'DRIVER': 'UNKNOWN',
}

for unit_type, replacement_value in unit_type_occupant_count_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'OCCUPANT_CNT', replacement_value)


# Replace null values in the 'FIRST_CONTACT_POINT' column based on 'UNIT_TYPE' conditions
unit_type_first_contact_point_replacements = {
    'PEDESTRIAN': 'NON APPLICABLE',
    'BICYCLE': 'NON APPLICABLE',
    'NON-MOTOR VEHICLE': 'NON APPLICABLE',
    'NON-CONTACT VEHICLE': 'UNKNOWN',
    'DRIVER': 'UNKNOWN',
}

for unit_type, replacement_value in unit_type_first_contact_point_replacements.items():
    replace_nulls_with_conditions(vehicles, 'UNIT_TYPE', unit_type, 'FIRST_CONTACT_POINT', replacement_value)
