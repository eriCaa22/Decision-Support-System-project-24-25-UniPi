from Utility_pop import connect_to_db, populate_database, populate_database_with_mapping, column_mapping
import os

# Main function
def main():
    # Stabilisce la connessione al database
    connection = connect_to_db()
    if not connection:
        print("Database connection failed.")
        return

    # Directory contenente i file CSV
    csv_directory = "C:/Users/al797/Documents/GitHub/LDS-project-24-25"

    # Mappatura file-tabella e funzione
    file_table_mapping = {
        "Geography.csv": (populate_database, "GEOGRAPHY"),  # fatto
        "Crash_date.csv": (populate_database, "CRASH_DATE"),  # fatto
        "Cause.csv": (populate_database, "CAUSE"),  # fatto
        "Vehicle.csv": (populate_database, "VEHICLE_C"),  # fatto
        "Damage.csv": (populate_database_with_mapping, "FACT_DAMAGE"),
        "Crash.csv": (populate_database, "CRASH"),  # fatto
        "Person.csv": (populate_database, "PERSON"),  # fatto
        "RoadCondition.csv": (populate_database, "ROAD_CONDITION"),  # fatto
    }

    # Itera sui file nella mappatura
    for file_name, (function, table) in file_table_mapping.items():
        file_path = os.path.join(csv_directory, file_name)

        try:
            if function == populate_database:
                print(f"Populating table {table} from file {file_name}...")
                function(file_path, table, connection)
            elif function == populate_database_with_mapping:
                print(f"Populating table {table} with column mapping from file {file_name}...")
                function(file_path, table, connection, column_mapping)

        except Exception as e:
            print(f"Error populating table {table} from file {file_name}: {e}")

    # Chiude la connessione al database
    connection.close()
    print("Database population complete!")

if __name__ == "__main__":
    main()


