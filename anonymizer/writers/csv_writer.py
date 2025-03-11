import csv
from pathlib import Path
import ipdb

def write_dicts_to_csv(file_path, dict_list):
    """
    Writes a list of dictionaries to a CSV file.
    """
    if not dict_list:
        print("The list of dictionaries is empty.")
        return

    # Get the headers from the first dictionary keys
    headers = dict_list[0].keys()
    try:
        with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            
            # Write the header
            writer.writeheader()
            
            # Write the data
            writer.writerows(dict_list)
                
        print(f"Data successfully written to {file_path}.")
    except Exception as e:
        print(f"An error occurred while writing to the file: {e}")


def write_dicts_to_multiple_csv(file_path, dict_list, file_name, size=200000):
    """
    Writes a list of dictionaries to a CSV file.
    """
    if not dict_list:
        print("The list of dictionaries is empty.")
        return

    # Get the headers from the first dictionary keys
    headers = dict_list[0].keys()
    for i in range(0, int(len(dict_list)/size) + 1):
        try:
            file = file_path + '/' + str(i) + '_' + file_name
            with open(file, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                
                # Write the header
                writer.writeheader()
                
                # Write the data
                writer.writerows(dict_list[i * size : (i + 1) * size])
                    
            print(f"{i}, Data successfully written to {file_path}.")
        except Exception as e:
            print(f"An error occurred while writing to the file: {e}")
