import csv


# Funzione per leggere i file e indicizzarli
def indicizza_file(filepath, key_col):
    indicizzati = {}
    with open(filepath, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = row[key_col]
            indicizzati[key] = row
    return indicizzati


# Funzione per creare il dataset finale
def merge_files(people_file, vehicles_file, crashes_file, key_col, output_file):
    # Indicizziamo i file Vehicles e Crashes
    vehicles_data = indicizza_file(vehicles_file, key_col)
    crashes_data = indicizza_file(crashes_file, key_col)

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


# Percorsi dei file
people_file = 'People_filled.csv'
vehicles_file = 'Vehicles_filled.csv'
crashes_file = 'Crashes_filled.csv'
output_file = 'Merged_output.csv'

# Esegui il merge
merge_files(people_file, vehicles_file, crashes_file, 'RD_NO', output_file)

print(f"Dataset finale salvato in {output_file}")
