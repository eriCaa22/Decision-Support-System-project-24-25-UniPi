from Utility_pop import connect_to_db, populate_database
import os

# Main function
def main():
    # Stabilisce la connessione al database
    connection = connect_to_db()
    if not connection:
        return

    # Directory contenente i file CSV
    csv_directory = "C:/Users/al797/Documents/GitHub/LDS-project-24-25"

    # Mappatura file-tabella
    file_table_mapping = {
        #"Geography.csv": "GEOGRAPHY",  # fatto
        #"Crash_date.csv": "CRASH_DATE",  # fatto
        #"Cause.csv": "CAUSE",  # fatto
        # "Damage.csv": "FACT_DAMAGE",  # BOH
        # "Vehicle.csv": "VEHICLE",  # fatto
        "Crash.csv": "CRASH",  # fatto
        # "Person.csv": "PERSON",  # fatto
        "RoadCondition.csv": "ROAD_CONDITION",  # fatto
    }

    # Itera sui file e popola il database
    for file_name, table_name in file_table_mapping.items():
        file_path = os.path.join(csv_directory, file_name)
        if os.path.exists(file_path):
            print(f"Processing file: {file_name}")
            populate_database(file_path, table_name, connection)
        else:
            print(f"File not found: {file_path}")

    # Chiude la connessione al database
    connection.close()
    print("Database population complete!")

if __name__ == "__main__":
    main()