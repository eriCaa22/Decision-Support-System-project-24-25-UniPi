#%%
import csv

with open('C:/Users/al797/Desktop/People.csv', mode='r') as file:
    reader = csv.DictReader(file)
    people = [row for row in reader]
#%%
missing_counts = {column: 0 for column in people[0].keys()}

for row in people:
    for column, value in row.items():
        if value == '' or value is None: 
            missing_counts[column] += 1

for column, count in missing_counts.items():
    print(f"Colonna '{column}': {count} valori mancanti")
#%%
def replace_nulls(ds, column_name, replacement_value):
    
    for record in ds:
        value = record[column_name].strip().lower() if record[column_name] else None
        
        if value in [None, "", "null", "none"]:
            record[column_name] = replacement_value
#%%
def replace_nulls_with_conditions(ds, column_name_to_check, target_value, column_to_replace, replacement_value):
    for record in ds:
        
        if record.get(column_name_to_check) == target_value and (not record.get(column_to_replace) or record[column_to_replace] in ["", "null", "none", "NaN"]):
            record[column_to_replace] = replacement_value

#%%
def mv_count(variable):
    count = 0 
    for record in people:
        if not record[variable]:
            count += 1
    return count
#%% md
# # Vehicles ID
#%% md
# #### SOSTITUISCO CON 0 Poichè non hanno la targa(Pedoni/bici..)
#%%
mv_count('VEHICLE_ID')
#%%
replace_nulls(vehicles, 'UNIT_TYPE', -1.0)
#%%
mv_count('VEHICLE_ID')
#%% md
# # City
#%% md
# #### SOSTITUISCO con CHICAGO (perchè è la moda)
#%%
mv_count('CITY')
#%%
replace_nulls(people, 'CITY', 'CHICAGO')
#%%
mv_count('CITY')
#%% md
# # State
#%% md
# #### SOSTITUISCO con MODA
#%%
mv_count('STATE')
#%%
replace_nulls(people, 'STATE', 'IL')
#%%
mv_count('STATE')
#%% md
# # Sex
#%% md
# #### SOSTITUISCO con la Moda
#%%
mv_count('SEX')
#%%
replace_nulls(people, 'SEX', 'M')
#%%
mv_count('SEX')
#%% md
# # Age
#%%
mv_count('AGE')
#%% md
# #### SOSTITUISCO con Mediana perchè la distribuzione non è normale
#%%
null_count = 0
for record in people:
    if not record["AGE"]:  
        null_count += 1
print(null_count)
#%%
replace_nulls(people, 'AGE', '36.0')
#%%
mv_count('AGE')
#%% md
# # Safety_Equipment
#%%
mv_count('SAFETY_EQUIPMENT')
#%% md
# #### SOSTITUISCO con USAGE UNKNOWN, (si potrebbero anche eliminare le righe), i pedoni/CICLISTI.... potrebbero avere come dispositivi di sicurezza delle giacche catarinfrangenti o simili
#%%
replace_nulls(people, 'SAFETY_EQUIPMENT', 'USAGE UNKNOWN')
#%%
mv_count('SAFETY_EQUIPMENT')
#%% md
# # Airbag_deployed
#%%
mv_count('AIRBAG_DEPLOYED')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'BICYCLE', 'AIRBAG_DEPLOYED', 'NOT APPLICABLE ')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'PEDESTRIAN', 'AIRBAG_DEPLOYED', 'NOT APPLICABLE ')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'NON-MOTOR VEHICLE', 'AIRBAG_DEPLOYED', 'NOT APPLICABLE ')
#%%
replace_nulls(people, 'PERSON_TYPE', 'DEPLOYMENT UNKNOWN')
#%%
mv_count('AIRBAG_DEPLOYED')
#%% md
# #### SOPRA ho rimosso tutte le persone che non possono avere AIRBAG, per le righe rimanenti lasciamo NaN
#%% md
# # Ejection
#%% md
# #### Sostituisco con NO VEHICLE in quanto non erano sui veicoli
#%%
mv_count('EJECTION')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'PASSENGER', 'EJECTION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'NON-CONTACT VEHICLE', 'EJECTION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'BICYCLE', 'EJECTION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'PEDESTRIAN', 'EJECTION', 'NOT APPLICABLE')
#%%
replace_nulls_with_conditions(people, 'PERSON_TYPE', 'NON-MOTOR VEHICLE', 'EJECTION', 'UNKNOWN')
#%%
mv_count('EJECTION')
#%% md
# # Injury_classification
#%% md
# #### SOSTITUISCO con NO INDICATION OF INJURY
#%%
mv_count('INJURY_CLASSIFICATION')
#%%
replace_nulls(people, 'INJURY_CLASSIFICATION', 'UNKNOWN')
#%%
mv_count('INJURY_CLASSIFICATION')
#%% md
# # Driver_action
#%%
mv_count('DRIVER_ACTION')
#%% md
# #### SOSTITUISCO con NONE i NON GUIDATORI e con UNKNOWN i guidatori di NON CONTACT VEHICLE
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'PASSENGER', 'DRIVER_ACTION', 'NOT APPLICABLE')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'PEDESTRIAN', 'DRIVER_ACTION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'BICYCLE', 'DRIVER_ACTION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'NON-MOTOR VEHICLE', 'DRIVER_ACTION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'NON-CONTACT VEHICLE', 'DRIVER_ACTION', 'UNKNOWN')
#%%
mv_count('DRIVER_ACTION')
#%% md
# # Driver_vision
#%%
mv_count('DRIVER_VISION')
#%% md
# #### SOSTITUISCO con NOT APPLICABLE perchè nessuno guida un veicolo tranne quelli NON_CONTACT VEHICLE in cui metto UNKNOWN
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'PASSENGER', 'DRIVER_VISION', 'NOT APPLICABLE')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'PEDESTRIAN', 'DRIVER_VISION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'BICYCLE', 'DRIVER_VISION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'NON-MOTOR VEHICLE', 'DRIVER_VISION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'NON-CONTACT VEHICLE', 'DRIVER_VISION', 'UNKNOWN')
#%%
mv_count('DRIVER_VISION')
#%% md
# # Physical_condition
#%% md
# #### SOSTITUOSCO con UNKNOWN i guidatori di non contact vehicle, Not applicable il resto
# 
#%%
mv_count('PHYSICAL_CONDITION')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'PASSENGER', 'PHYSICAL_CONDITION', 'NOT APPLICABLE')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'PEDESTRIAN', 'PHYSICAL_CONDITION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'BICYCLE', 'PHYSICAL_CONDITION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'NON-MOTOR VEHICLE', 'PHYSICAL_CONDITION', 'UNKNOWN')
#%%
replace_nulls_with_conditions(people,'PERSON_TYPE', 'NON-CONTACT VEHICLE', 'PHYSICAL_CONDITION', 'UNKNOWN')
#%%
mv_count('PHYSICAL_CONDITION')
#%% md
# # BAC_result
#%% md
# #### SOSTITUISCO con NOt applicable in quanto sono tutti passeggeri
#%%
mv_count('BAC_RESULT')
#%%
replace_nulls(people, 'BAC_RESULT', 'NOT APPLICABLE')
#%%
mv_count('BAC_RESULT')
#%% md
# # Damage
#%% md
# #### SOSTITUISCO con MEDIA
#%%
mv_count('DAMAGE')
#%%
replace_nulls(people, 'DAMAGE', '250.00')
#%%
mv_count('DAMAGE')
#%% md
# #### APPROSSIMO a due cifre decimali i costi
#%%
for record in people:
    if record["DAMAGE"] is not None:  
        try:
            record["DAMAGE"] = round(float(record["DAMAGE"]), 2)
        except ValueError:
            
            pass
#%% md
# # FINE
#%%
file_path = "C:/Users/al797/DSS LAB/cleaned_people.csv"  

with open(file_path, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=people[0].keys())
    writer.writeheader()
    writer.writerows(people)

print(f"File salvato con successo in: {file_path}")
#%%
