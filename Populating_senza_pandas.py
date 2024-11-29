import csv
import pyodbc
import os

SERVER = 'lds.di.unipi.it'
DATABASE = 'Group_ID_24_DB'
USERNAME = 'Group_ID_24'
PASSWORD = 'IMTGP44N'
DRIVER = 'ODBC Driver 17 for SQL Server'


# Connect to the database
def connect_to_db():
    try:
        connection = pyodbc.connect(
            f'DRIVER={{{DRIVER}}};'
            f'SERVER={SERVER};'
            f'DATABASE={DATABASE};'
            f'UID={USERNAME};'
            f'PWD={PASSWORD}'
        )
        print("Database connection successful!")
        return connection
    except Exception as e:
        print("Error connecting to database:", e)
        return None


# Read and insert CSV data
def populate_database(file_path, table_name, connection):
    try:
        # Open the CSV file
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)

            # Get the column names from the first row
            columns = next(reader)  # Read the header row
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))

            # Prepare the insert statement
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # Create a cursor
            cursor = connection.cursor()

            # Insert rows in batches
            batch_size = 1000
            batch = []
            total_inserted = 0  # Tracks the total number of rows inserted

            for row in reader:
                batch.append(row)
                if len(batch) == batch_size:
                    cursor.executemany(insert_sql, batch)
                    connection.commit()
                    total_inserted += len(batch)
                    print(f"Inserted {total_inserted} rows into {table_name}")
                    batch = []

            # Insert any remaining rows
            if batch:
                cursor.executemany(insert_sql, batch)
                connection.commit()
                print(f"Inserted {len(batch)} rows into {table_name}")

        print(f"Data successfully inserted into {table_name}!")
    except Exception as e:
        print(f"Error inserting data into {table_name}:", e)


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
