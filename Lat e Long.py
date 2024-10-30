# -*- coding: utf-8 -*-
import utils as utils
import queries as queries
from geopy.geocoders import Nominatim



# Funzione per ottenere latitudine e longitudine a partire dal nome della strada
def getCoordinatesFromAddress(street_name, city="Chicago", state="IL", geolocator=Nominatim(user_agent="LDS_Group_24")):
    """
    Retrieves latitude and longitude for a given street name in Chicago.

    Parameters:
    - street_name: Name of the street (e.g., "Magnificent Mile").
    - city (optional): City to restrict the search, default is "Chicago".
    - state (optional): State to restrict the search, default is "IL".
    - geolocator (optional): Geopy Nominatim geolocator object. Default is a new geolocator with a specified user agent.

    Returns:
    A dictionary with latitude and longitude, or None if the location is not found.
    """
    # Crea un indirizzo completo con città e stato per restringere la ricerca
    address = f"{street_name}, {city}, {state}"

    # Esegue la geocodifica usando l'indirizzo
    location = geolocator.geocode(address)

    # Verifica se la posizione è stata trovata e restituisce le coordinate
    if location:
        return {"latitude": location.latitude, "longitude": location.longitude}
    else:
        return None


# Funzione per aggiornare le coordinate basate sull'indirizzo di una strada a Chicago nel database
def setChicagoStreetCoordinates(**kwargs):
    """
    Updates database with latitude and longitude for records with street names in Chicago using geocoding.

    Parameters:
    - conn (optional): An optional parameter for providing an existing database connection. If not provided, a new connection will be established.
    - count (optional): The maximum number of records to process. If not provided or set to -1, all records will be processed.
    - batch_size (optional): The number of records to process in each batch. Default is 1000.

    Returns:
    None
    """
    conn = utils.connectDB() if "conn" not in kwargs.keys() else kwargs["conn"]
    count = -1 if "count" not in kwargs.keys() else kwargs["count"]
    batch_size = 1000 if "batch_size" not in kwargs.keys() else kwargs["batch_size"]

    select_cursor = conn.cursor()
    insert_cursor = conn.cursor()
    insert_cursor.rollback()

    # Esegue la query per ottenere record che richiedono geocodifica a Chicago
    street_names_res = select_cursor.execute(queries.select_empty_geo_dimension_with_street_names)
    rows = street_names_res.fetchmany(batch_size)
    i = 0

    # Ciclo per aggiornare i record con le informazioni di latitudine e longitudine
    while len(rows) > 0 and (count == -1 or i < count):
        for r in rows:
            if count == -1 or i < count:
                # Esegue la geocodifica per ottenere latitudine e longitudine dalla strada
                street_name = r[1]  # Assumiamo che il nome della strada sia nel secondo campo
                coordinates = getCoordinatesFromAddress(street_name)

                if coordinates:
                    # Aggiorna il database con latitudine e longitudine
                    insert_cursor.execute(
                        queries.update_geo_location_with_coordinates,
                        (coordinates["latitude"], coordinates["longitude"], r[0])  # r[0] è l'ID del record
                    )
                    i += 1
        # Mostra il progresso
        utils.printUpdate(i, "Street Geography in Chicago")
        rows = street_names_res.fetchmany(batch_size)

    insert_cursor.close()
    conn.close()
