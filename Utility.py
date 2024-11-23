########################
# IMPORT LIBRARIES
########################

import csv
import csv

def load_csv(filepath):
    with open(filepath, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def save_to_csv(data, file_path):
    if not data:
        print(f"Dataset vuoto, impossibile salvare il file: {file_path}")
        return

    # Ottieni i nomi delle colonne dal primo dizionario
    columns = data[0].keys()

    # Scrivi i dati in un file CSV
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)

        # Scrivi l'intestazione
        writer.writeheader()

        # Scrivi i dati riga per riga
        writer.writerows(data)
    print(f"File salvato correttamente: {file_path}")

def check_null_values(data):
    # Dizionario per contare i valori nulli per ogni colonna
    null_counts = {}

    # Valori considerati nulli
    null_values = [None, "", " ", "null", "none", "nan", "NaN"]

    # Itera su ogni riga del dataset
    for row in data:
        for column, value in row.items():
            # Se il valore è nullo, aumenta il conteggio per la colonna
            if value in null_values:
                null_counts[column] = null_counts.get(column, 0) + 1

    # Ritorna il dizionario con il conteggio dei valori nulli
    return null_counts

#######################
# FUNCTION DEFINITIONS
########################
"""
# Caricamento del dataset
def load_csv(filepath):
    with open(filepath, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def save_to_csv(data, file_path):
    if not data:
        print(f"Dataset vuoto, impossibile salvare il file: {file_path}")
        return

    # Ottieni i nomi delle colonne dal primo dizionario
    columns = data[0].keys()

    # Scrivi i dati in un file CSV
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)

        # Scrivi l'intestazione
        writer.writeheader()

        # Scrivi i dati riga per riga
        writer.writerows(data)
    print(f"File salvato correttamente: {file_path}")

def print_rows_with_null_location(ds):
    # Identifica i valori considerati come nulli
    null_values = [None, " ", "null", "none", "nan", "NaN", ""]

    # Filtra le righe con 'unit type' nullo
    null_rows = [record for record in ds if record.get("LOCATION").strip().lower() in null_values]

    # Stampa le righe con 'unit type' nullo
    if null_rows:
        print("Righe con 'unit type' nullo:")
        for row in null_rows:
            print(row)
    else:
        print("Non ci sono righe con 'unit type' nullo.")

def check_null_values(data):
    # Dizionario per contare i valori nulli per ogni colonna
    null_counts = {}

    # Valori considerati nulli
    null_values = [None, "", " ", "null", "none", "nan", "NaN"]

    # Itera su ogni riga del dataset
    for row in data:
        for column, value in row.items():
            # Se il valore è nullo, aumenta il conteggio per la colonna
            if value in null_values:
                null_counts[column] = null_counts.get(column, 0) + 1

    # Ritorna il dizionario con il conteggio dei valori nulli
    return null_counts
"""