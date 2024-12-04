from Utility_pop import connect_to_db, populate_database, populate_database_with_mapping, column_mapping
import os
import pyodbc


# Main function
def main():
    connection = connect_to_db()
    if not connection:
        return

    # Directory contenente i file CSV
    csv_directory = "C:/Users/al797/Documents/GitHub/LDS-project-24-25"

    # Mappatura file-tabella
    file_table_mapping = {
        "Geography.csv": (populate_database, "GEOGRAPHY"),  # fatto
        "Crash_date.csv": (populate_database, "CRASH_DATE"),  # fatto
        "Cause.csv": (populate_database, "CAUSE"),  # fatto
        "Vehicle.csv": (populate_database, "VEHICLE"),  # fatto
        "Damage_2.csv": (populate_database_with_mapping, "FACT_DAMAGE"),
        "Crash.csv": (populate_database, "CRASH"),  # fatto
        "Person.csv": (populate_database, "PERSON"),  # fatto
        "RoadCondition.csv": (populate_database, "ROAD_CONDITION"),  # fatto
    }

    for file_name, (function_name, table) in file_table_mapping.items():
        file_path = os.path.join(csv_directory, file_name)
        if function_name == 'populate_database':
            function_name(file_path, table, connection)
        if function_name == 'populate_database_with_mapping':
            function_name(file_path, table, connection, column_mapping)

    # Chiude la connessione al database
    connection.close()
    print("Database population complete!")

if __name__ == "__main__":
    main()