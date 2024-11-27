from geopy.geocoders import Nominatim
import time

# REMOVE ROWS WITH NULL VALUE FOR THE SPECIFIED CCOLUMN
def remove_null_rows(data, column_name):
    null_values = [None, "", " ", "null", "none", "nan", "NaN"]
    return [row for row in data if row.get(column_name) not in null_values]

# REPLACE NULL VALUES IN THE SPECIFIED COLUMN WITH THE SPECIFIED VALUE
def replace_nulls(ds, column_name, replacement_value):
    for record in ds:
        if column_name in record:
            value = record[column_name]
            if value is None or (isinstance(value, str) and value.strip().lower() in ["", "null", "none"]):
                record[column_name] = replacement_value
    return ds


# REPLACE NULL VALUES IN THE SPECIFIED COLUMN WITH THE SPECIFIED VALUE IF ANOTHER COLUMN HAS A SPECIFIC VALUE
def replace_nulls_with_conditions(ds, column_name_to_check, target_value, column_to_replace, replacement_value):
    for record in ds:
        if record.get(column_name_to_check) == target_value and not record.get(column_to_replace):
            record[column_to_replace] = replacement_value
    return ds


def apply_replacements(dataset, replacements_dict, column_check):
    for column, conditions in replacements_dict.items():
        for unit_type, replacement_value in conditions.items():
            dataset = replace_nulls_with_conditions(dataset, column_check, unit_type, column, replacement_value)
    return dataset


# FUNCTIONS TO FILL GEOLOCATION
geolocator = Nominatim(user_agent="incident_locator")
def fill_missing_geolocation(data, street_no_col, street_dir_col, street_name_col, lat_col, lon_col, loc_col,
                             city="Chicago", state="IL", pause=1):
    """
    Parameters:
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
            address = f"{street_no} {street_direction} {street_name}, {city}, {state}"
            location = geolocator.geocode(address, timeout=10)
            if location:
                return location.latitude, location.longitude, location.address
        except Exception as e:
            print(f"Errore con l'indirizzo {address}: {e}")
        return None

    for record in data:
        if not record.get(lat_col) or not record.get(lon_col) or not record.get(loc_col):
            result = get_lat_lon_location(record.get(street_no_col), record.get(street_dir_col),
                                          record.get(street_name_col))
            if result:
                lat, lon, loc = result
                if lat and lon:

                    record[lat_col] = lat
                    record[lon_col] = lon
                    record[loc_col] = loc

            time.sleep(pause)

    return data

from collections import defaultdict

def calculate_mean_coordinates(dataset, street_name, latitude, longitude):

    street_coordinates = defaultdict(list)

    # Itera sul dataset per raccogliere le coordinate
    for row in dataset:
        street_name = row.get(street_name)
        latitude = row.get(latitude)
        longitude = row.get(longitude)

        # Verifica che i valori siano validi
        if street_name and latitude is not None and longitude is not None:
            street_coordinates[street_name].append((latitude, longitude))

    # Calcola la media delle coordinate per ogni strada
    street_mean_coordinates = {}
    for street_name, coords in street_coordinates.items():
        if coords:  # Verifica che ci siano coordinate valide
            avg_latitude = sum(coord[0] for coord in coords) / len(coords)
            avg_longitude = sum(coord[1] for coord in coords) / len(coords)
            street_mean_coordinates[street_name] = {
                'latitude': avg_latitude,
                'longitude': avg_longitude
            }

    return street_mean_coordinates



