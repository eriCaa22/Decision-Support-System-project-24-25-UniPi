from Utility import connect_to_db, populate_database
import os

# Main function
def main():
    # Establish database connection
    connection = connect_to_db()
    if not connection:
        return

    # Path to the directory containing CSV files
    csv_directory = "C:/Users/al797/Documents/GitHub/LDS-project-24-25"

    # Map of file names to table names
    file_table_mapping = {
        #"Geography.csv": "GEOGRAPHY", # fatto
        #"Crash_date.csv": "CRASH_DATE",
        #"Cause.csv": "CAUSE", #fatto
        #"Damage.csv": "FACT_DAMAGE", #BOH
        #"Vehicle.csv": "VEHICLE", #DICE CHE CI SONO DEI DUPLICATI NON CAPISCO DOVE
        #"Crash.csv": "CRASH",
        "Person.csv": "PERSON",
        #"RoadCondition.csv": "ROAD_CONDITION", #fatto

    }

    # Iterate over files and populate the database
    for file_name, table_name in file_table_mapping.items():
        file_path = os.path.join(csv_directory, file_name)
        if os.path.exists(file_path):
            print(f"Processing file: {file_name}")
            populate_database(file_path, table_name, connection)
        else:
            print(f"File not found: {file_path}")

    # Close the connection
    connection.close()
    print("Database population complete!")


if __name__ == "__main__":
    main()
