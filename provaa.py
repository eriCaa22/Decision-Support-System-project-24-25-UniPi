import csv

from jupyterlab.semver import valid


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


# Colonne da mantenere
col_to_keep = ['VEHICLE_ID', 'BAC_RESULT']

# Filtra il dataset e salva in un nuovo file
filter_columns('LDS24 - Data/Merged_output.csv', 'LDS24 - Data/prova.csv', col_to_keep)
print("Dataset filtrato creato correttamente.")











