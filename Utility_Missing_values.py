########################
# IMPORT LIBRARIES
########################
from geopy.geocoders import Nominatim
import time

########################
# FUNCTION DEFINITIONS
########################

def remove_null_rows(data, column_name):
    null_values = [None, "", " ", "null", "none", "nan", "NaN"]  # Valori considerati nulli
    return [row for row in data if row.get(column_name) not in null_values]

def replace_nulls(ds, column_name, replacement_value):
    for record in ds:
        if column_name in record:
            value = record[column_name]
            if value is None or value.strip().lower() in ["", "null", "none"]:
                record[column_name] = replacement_value
    return ds  # Restituisce il dataset modificato

def replace_nulls_with_conditions(ds, column_name_to_check, target_value, column_to_replace, replacement_value):
    for record in ds:
        if record.get(column_name_to_check) == target_value and not record.get(column_to_replace):
            record[column_to_replace] = replacement_value
    return ds  # Restituisce il dataset modificato

def apply_replacements(dataset, replacements_dict, column_check):
    for column, conditions in replacements_dict.items():
        for unit_type, replacement_value in conditions.items():
            dataset = replace_nulls_with_conditions(dataset, column_check, unit_type, column, replacement_value)
    return dataset  # Restituisce il dataset modificato



""""
def remove_null_rows(data, column_name):
    null_values = [None, "", " ", "null", "none", "nan", "NaN"]  # Valori considerati nulli
    filtered_data = [row for row in data if row.get(column_name) not in null_values]
    return filtered_data


# Function to replace null values in a column with a specified value
def replace_nulls(ds, column_name, replacement_value):
      for record in ds:
        if record[column_name]:
            value = record[column_name].strip().lower()
            if value in [None, "", "null", "none"]:
                record[column_name] = replacement_value


# Function to replace null values in a column with a specified value, based on a specific condition from the same ds
def replace_nulls_with_conditions(ds, column_name_to_check, target_value, column_to_replace, replacement_value):
    for record in ds:
        if record[column_name_to_check] == target_value and not record[column_to_replace]:
            record[column_to_replace] = replacement_value

# Function to remove rows with null values in a specific column
def remove_rows_with_nulls(data, attribute):
    data[:] = [record for record in data if record.get(attribute) not in [None, "", float("nan")]]


# Function to replace missing values in a column with values from another dataset
def replace_missing_with_value_from_another_df(
        main_df, lookup_df, join_column, target_column_main, source_column_lookup
):
    # Creiamo un dizionario da lookup_df usando join_column come chiave e source_column_lookup come valore
    lookup_dict = {record[join_column]: record[source_column_lookup] for record in lookup_df if
                   source_column_lookup in record}

    # Ciclo su ciascun record in main_df
    for record in main_df:
        # Controlla se il valore in target_column_main è mancante (None o "")
        if not record.get(target_column_main):  # True se è None, "" o altri valori falsy
            rd_no = record.get(join_column)
            if rd_no and rd_no in lookup_dict:  # Controlla se RD_NO è valido e presente in lookup_dict
                record[target_column_main] = lookup_dict[rd_no]  # Sostituisce il valore mancante


# Funzione generica per applicare sostituzioni condizionali a più colonne
def apply_replacements(dataset, replacements_dict, column_check):
    for column, conditions in replacements_dict.items():
        for unit_type, replacement_value in conditions.items():
            replace_nulls_with_conditions(dataset, column_check, unit_type, column, replacement_value)



"""

# LONGITUDINE E LATITUDINE

    # Configura il geolocalizzatore
geolocator = Nominatim(user_agent="incident_locator")


#  Completa i dati di latitudine, longitudine e indirizzo mancante in un dataset.

""" def fill_missing_geolocation(data, street_no_col, street_dir_col, street_name_col, lat_col, lon_col, loc_col,
                                 city="Chicago", state="IL", pause=1):
        
        Parametri:

        - data: Lista di record (dataset) su cui operare.
        - street_no_col: Nome della colonna del numero civico.
        - street_dir_col: Nome della colonna della direzione della strada.
        - street_name_col: Nome della colonna del nome della strada.
        - lat_col: Nome della colonna per la latitudine.
        - lon_col: Nome della colonna per la longitudine.
        - loc_col: Nome della colonna per l'indirizzo completo.
        - city: Città di default per costruire l'indirizzo (default: Chicago).
        - state: Stato di default per costruire l'indirizzo (default: IL).
        - pause: Pausa (in secondi) tra le richieste per evitare il limite API (default: 1).
        

        # Funzione interna per geocodificare un indirizzo
        def get_lat_lon_location(street_no, street_direction, street_name):
            # If you want to print the addresses for which we can't find lat, long and address uncomment the commented line below
            # try:
                # Crea l'indirizzo come stringa
                address = f"{street_no} {street_direction} {street_name}, {city}, {state}"
                location = geolocator.geocode(address, timeout=10)
                if location:
                    return location.latitude, location.longitude, location.address
           # except Exception as e:
             #   print(f"Errore con l'indirizzo {address}: {e}")
           # return None, None, None

        # Itera sui record del dataset
        for record in data:
            if not record.get(lat_col) or not record.get(lon_col) or not record.get(loc_col):
                lat, lon, loc = get_lat_lon_location(record.get(street_no_col), record.get(street_dir_col),
                                                     record.get(street_name_col))
                if lat and lon:
                    # Aggiorna i valori mancanti
                    record[lat_col] = lat
                    record[lon_col] = lon
                    record[loc_col] = loc

                # Pausa per evitare limiti di richiesta
                time.sleep(pause)

        return data  # Restituisce il dataset aggiornato
"""

def fill_missing_geolocation(data, street_no_col, street_dir_col, street_name_col, lat_col, lon_col, loc_col,
                             city="Chicago", state="IL", pause=1):
    """
    Parametri:

    - data: Lista di record (dataset) su cui operare.
    - street_no_col: Nome della colonna del numero civico.
    - street_dir_col: Nome della colonna della direzione della strada.
    - street_name_col: Nome della colonna del nome della strada.
    - lat_col: Nome della colonna per la latitudine.
    - lon_col: Nome della colonna per la longitudine.
    - loc_col: Nome della colonna per l'indirizzo completo.
    - city: Città di default per costruire l'indirizzo (default: Chicago).
    - state: Stato di default per costruire l'indirizzo (default: IL).
    - pause: Pausa (in secondi) tra le richieste per evitare il limite API (default: 1).
    """

    # Funzione interna per geocodificare un indirizzo
    def get_lat_lon_location(street_no, street_direction, street_name):
        try:
            # Crea l'indirizzo come stringa
            address = f"{street_no} {street_direction} {street_name}, {city}, {state}"
            location = geolocator.geocode(address, timeout=10)
            if location:
                return location.latitude, location.longitude, location.address
        except Exception as e:
            print(f"Errore con l'indirizzo {address}: {e}")
        return None  # Restituisci None se si verifica un errore o se location non è valido

    # Itera sui record del dataset
    for record in data:
        if not record.get(lat_col) or not record.get(lon_col) or not record.get(loc_col):
            result = get_lat_lon_location(record.get(street_no_col), record.get(street_dir_col),
                                          record.get(street_name_col))
            if result:  # Verifica se result non è None
                lat, lon, loc = result
                if lat and lon:
                    # Aggiorna i valori mancanti
                    record[lat_col] = lat
                    record[lon_col] = lon
                    record[loc_col] = loc

            # Pausa per evitare limiti di richiesta
            time.sleep(pause)

    return data


