from pathlib import Path
import random
import csv

# List of column names to search for
columns = [
    "ARCHIVE_USER_NAME",
    "HOST_USER_NAME",
    "LDAP_USER_NAME",
    "LOGIN_USER_NAME",
    "USER_NAME",
    "USER_NAME__LONG_",
    "LOGON_USER_NAME",
    "EXCHANGE_SERVICE_USERNAME"
]


def get_columns_from_csv(file):
    """Reads a CSV file."""
    try:
        with open(file, mode='r', newline='') as csvfile:
            csvfile.__next__()  # skip first row as it is a comment
            reader = csv.DictReader(csvfile)
            if reader.fieldnames is not None:
                return list(reader.fieldnames)
            else:
                print("The CSV file is empty or does not contain headers.")
    except FileNotFoundError:
        print(f"The file '{file}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
        print(file)
    return []


def get_data_from_csv(file):
    """Reads a CSV file."""
    try:
        with open(file, mode='r', newline='') as csvfile:
            csvfile.__next__()  # skip first row as it is a comment
            reader = csv.DictReader(csvfile)
            if reader.fieldnames is not None:
                return list(reader)
            else:
                print("The CSV file is empty or does not contain headers.")
    except FileNotFoundError:
        print(f"The file '{file}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
        print(file)
    return []

def find_csv_files_with_columns(directory):
    # Convert the columns list to a set for faster lookups
    columns_set = set(columns)
    # Create a Path object for the directory
    directory_path = Path(directory)
    # List to store the names of CSV files that contain any of the columns
    matching_files = []

    # Iterate over all CSV files in the directory
    for csv_file in directory_path.glob("*.csv"):
        field_names = get_columns_from_csv(csv_file)
        if columns_set.intersection(field_names):
            matching_files.append(csv_file.name)

    return matching_files


def get_all_usernames(directory, matching_files):
    # Convert the columns list to a set for faster lookups
    columns_set = set(columns)
    # Create a Path object for the directory
    directory_path = Path(directory)
    # List to store the names of CSV files that contain any of the columns
    values = []
    # Iterate over all CSV files in the directory
    for csv_file in matching_files:
        fpath = directory_path / csv_file
        data = get_data_from_csv(fpath)

        # List of keys you want to extract values for
        keys_to_extract = ["name", "city", "email"]

        # Extract values for the specified keys
        extracted_values = [
            {key: item[key] for key in columns if key in item}
            for item in data
        ]
        values.extend(extracted_values)

    return values

# Example usage
directory = "/home/zaman/repos/data_anonymization/J1PROD"
matching_files = find_csv_files_with_columns(directory)

print(f"dir: {directory}")
print(f"files: {len(matching_files)}")

usernames = get_all_usernames(directory, matching_files)

keys = []
values = []

for item in usernames:
    keys.extend(item.keys())
    values.extend(item.values())

random.shuffle(values)










def modify_csv_files_in_directory(directory):
    anon_files = []
    # Convert columns list to a set for faster lookups
    columns_set = set(columns)
    # Create a Path object for the directory
    directory_path = Path(directory)

    # Iterate over all CSV files in the directory
    for csv_file in directory_path.glob("*.csv"):
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            # Check if any column in the file matches the columns we are looking for
            if columns_set.intersection(reader.fieldnames):
                anon_files.append(csv_file.name)
                # # Read the entire content of the file
                # rows = list(reader)

                # # Modify the values in the matching columns
                # for row in rows:
                #     for column in columns_set.intersection(reader.fieldnames):
                #         row[column] = modify_value(row[column])

                # # Write the modified content back to the CSV file
                # with open(csv_file, mode='w', newline='') as file:
                #     writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                #     writer.writeheader()
                #     writer.writerows(rows)
    return anon_files

def modify_value(value):
    # Example modification function - you can change this to whatever you need
    return random.choice(values)  # Converts the value to uppercase

# Example usage
anonymized = "/home/zaman/repos/data_anonymization/anonymized"
anon_files = modify_csv_files_in_directory(anonymized)
