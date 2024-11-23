########################
# IMPORT LIBRARIES
########################

import csv


########################
# FUNCTION DEFINITIONS
########################

# Caricamento del dataset
def load_csv(filepath):
    with open(filepath, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

