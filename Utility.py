########################
# IMPORT LIBRARIES
########################

import csv
import random



########################
# FUNCTION DEFINITIONS
########################

# Function to replace null values in a column with a specified value
def replace_nulls(ds, column_name, replacement_value):
    '''

    Parametri:
    - ds: Il dataset in cui effettuare la sostituzione
    - column_name: Il nome della colonna in cui effettuare la sostituzione
    - replacement_value: Il valore con cui sostituire i valori mancanti

    '''

    for record in ds:
        if record[column_name]:
            value = record[column_name].strip().lower()
        else:
            None

        if value in [None, "", "null", "none"]:
            record[column_name] = replacement_value


# Function to replace null values in a column with a specified value, based on a specific condition from the same ds
def replace_nulls_with_conditions(ds, column_name_to_check, target_value, column_to_replace, replacement_value):

    '''

    Parametri:
    - ds: Il dataset in cui effettuare la sostituzione
    - column_name_to_check: Il nome della colonna da controllare per la condizione
    - target_value: Il valore che deve essere presente in column_name_to_check per effettuare la sostituzione
    - column_to_replace: Il nome della colonna in cui effettuare la sostituzione
    - replacement_value: Il valore con cui sostituire i valori mancanti

    '''


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
    """

    Parametri:
    - main_df: Il dataset principale dove si desidera fare la sostituzione (ad esempio, 'vehicles').
    - lookup_df: Il dataset secondario da cui ottenere il valore di sostituzione (ad esempio, 'crashes').
    - join_column: La colonna usata per unire i due dataset (ad esempio, 'RD_NO').
    - target_column_main: La colonna nel main_df in cui sostituire i valori mancanti (ad esempio, 'Travel_direction').
    - source_column_lookup: La colonna in lookup_df che fornisce il valore di sostituzione (ad esempio, 'Street_direction').

    """

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


# Function to replace missing values in a column with random values with a fixed proportion
def assign_random_values(data, attribute, values, probabilities):

    '''
    Parameters:

    - data: The dataset in which to replace the missing values
    - attribute: The name of the column in which to replace the missing values
    - values: The list of values to choose from
    - probabilities: The list of probabilities for each value in the values list

    '''

    for record in data:
        if not record[attribute]:
            record[attribute] = random.choices(values, probabilities)[0]
