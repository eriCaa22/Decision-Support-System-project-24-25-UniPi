import csv


# LOAD DATASET
def load_csv(filepath):
    with open(filepath, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

#SAVE CSV
def save_to_csv(data, file_path):
    if not data:
        print(f"Dataset vuoto, impossibile salvare il file: {file_path}")
        return

    columns = data[0].keys()

    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)

        writer.writeheader()
        writer.writerows(data)
    print(f"File salvato correttamente: {file_path}")

# CHECK IF THERE ARE NULL VALUES
def check_null_values(data):
    null_counts = {}

    null_values = [None, "", " ", "null", "none", "nan", "NaN"]

    for row in data:
        for column, value in row.items():
            if value in null_values:
                null_counts[column] = null_counts.get(column, 0) + 1

    return "Valori: ", null_counts

# Funzione per leggere i file e indicizzarli
def index_file(filepath, key_column):
    indexed_data = {}
    with open(filepath, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = row[key_column]
            indexed_data[key] = row
    return indexed_data



# Funzione per creare il dataset finale
def merge_files(people_file, vehicles_file, crashes_file, key_col, output_file):
    # Indicizziamo i file Vehicles e Crashes
    vehicles_data = index_file(vehicles_file, key_col)
    crashes_data = index_file(crashes_file, key_col)

    # Apriamo il file People e scriviamo il dataset finale
    with open(people_file, mode='r', encoding='utf-8') as people, \
            open(output_file, mode='w', encoding='utf-8', newline='') as output:
        reader = csv.DictReader(people)
        fieldnames = reader.fieldnames + list(vehicles_data[next(iter(vehicles_data))].keys()) + \
                     list(crashes_data[next(iter(crashes_data))].keys())

        # Rimuovere colonne duplicate causate dal merge
        fieldnames = list(dict.fromkeys(fieldnames))

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rd_no = row[key_col]
            # Aggiungiamo i dati da Vehicles e Crashes se disponibili
            vehicle_info = vehicles_data.get(rd_no, {})
            crash_info = crashes_data.get(rd_no, {})

            # Combiniamo i dati in un'unica riga
            merged_row = {**row, **vehicle_info, **crash_info}
            writer.writerow(merged_row)

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