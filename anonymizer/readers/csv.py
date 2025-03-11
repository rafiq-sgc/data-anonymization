import csv
import sys
from rich import print
import ipdb

def read_csv(file):
    """Reads a CSV file."""
    try:
        csv.field_size_limit(sys.maxsize) # maximize field size
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

def read_csv_without_next(file):
    """Reads a CSV file."""
    try:
        csv.field_size_limit(sys.maxsize) # maximize field size
        with open(file, mode='r', newline='') as csvfile:
            # csvfile.__next__()  # skip first row as it is a comment
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

def read_csv_with_error(file):
    """Reads a CSV file."""
    try:
        csv.field_size_limit(sys.maxsize) # maximize field size
        # Open file in binary mode and replace null bytes
        with open(file, 'rb') as f:
            content = f.read().replace(b'\x00', b'')

        # Write the cleaned content back to a temporary file
        temp_file_path = file + '.tmp'
        with open(temp_file_path, 'wb') as f:
            f.write(content)
        with open(temp_file_path, mode='r', newline='') as csvfile:
            # csvfile.__next__()  # skip first row as it is a comment
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

def split_large_csv_to_multiple(data_file, save_dir):
    """Reads a CSV file."""
    # ipdb.set_trace()
    try:
        # csv.field_size_limit(sys.maxsize) # maximize field size
        with open(data_file, mode='r', newline='') as csvfile:
            csvfile.__next__()  # skip first row as it is a comment
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames

            if headers is not None:
                count = 0
                indx = 0
                dict_list = []
                file_name = data_file.split('/')[-1]
                for row in reader:
                    indx += 1
                    try:
                        dict_list.append(row)
                    except Exception as e:
                        # ipdb.set_trace()
                        print(e)

                    if indx % 100000 == 0:
                        count += 1
                        file = save_dir + '/' + str(count) + '_' + file_name
                        with open(file, mode='w', newline='', encoding='utf-8') as newfile:
                            writer = csv.DictWriter(newfile, fieldnames=headers)
                            # Write the header
                            writer.writeheader()
                            # Write the data
                            writer.writerows(dict_list)
                            print(f'{count} file write successful, {indx}')
                        dict_list = []
                count += 1
                file = save_dir + '/' + str(count) + '_' + file_name
                with open(file, mode='w', newline='', encoding='utf-8') as newfile:
                    writer = csv.DictWriter(newfile, fieldnames=headers)
                    # Write the header
                    writer.writeheader()
                    # Write the data
                    writer.writerows(dict_list)
                    print(f'{count} file write successfully done, {indx}')
            else:
                print("The CSV file is empty or does not contain headers.")
    except FileNotFoundError:
        print(f"The file '{file}' does not exist.")
    except Exception as e:
        # ipdb.set_trace()
        print(f"An error occurred: {e}")
        print(data_file)
    return []

def split_large_csv_to_multiple_with_null_error(data_file, save_dir):
    """Reads a CSV file."""
    # ipdb.set_trace()
    try:
        # Open file in binary mode and replace null bytes
        with open(data_file, 'rb') as f:
            content = f.read().replace(b'\x00', b'')

        # Write the cleaned content back to a temporary file
        temp_file_path = data_file + '.tmp'
        with open(temp_file_path, 'wb') as f:
            f.write(content)
        # csv.field_size_limit(sys.maxsize) # maximize field size
        with open(temp_file_path, mode='r', newline='') as csvfile:
            csvfile.__next__()  # skip first row as it is a comment
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames

            if headers is not None:
                count = 0
                indx = 0
                dict_list = []
                file_name = data_file.split('/')[-1]
                for row in reader:
                    indx += 1
                    try:
                        dict_list.append(row)
                    except Exception as e:
                        # ipdb.set_trace()
                        print(e)

                    if indx % 100000 == 0:
                        count += 1
                        file = save_dir + '/' + str(count) + '_' + file_name
                        with open(file, mode='w', newline='', encoding='utf-8') as newfile:
                            writer = csv.DictWriter(newfile, fieldnames=headers)
                            # Write the header
                            writer.writeheader()
                            # Write the data
                            writer.writerows(dict_list)
                            print(f'{count} file write successful, {indx}')
                        dict_list = []
                count += 1
                file = save_dir + '/' + str(count) + '_' + file_name
                with open(file, mode='w', newline='', encoding='utf-8') as newfile:
                    writer = csv.DictWriter(newfile, fieldnames=headers)
                    # Write the header
                    writer.writeheader()
                    # Write the data
                    writer.writerows(dict_list)
                    print(f'{count} file write successfully done, {indx}')
            else:
                print("The CSV file is empty or does not contain headers.")
    except FileNotFoundError:
        print(f"The file '{file}' does not exist.")
    except Exception as e:
        # ipdb.set_trace()
        print(f"An error occurred: {e}")
        print(data_file)
    return []