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

    return null_counts