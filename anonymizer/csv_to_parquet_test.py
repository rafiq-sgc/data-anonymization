'''
this script is to convert a csv file to a parquet file.
this csv files are j1 database table data.
we have a sql schema file of the database given by kaisar bhai.
we have to parse it and get all the tables column data type,
then convert the csv files data according to the data type and write parquet file with the data.
parquet file preserves the data type. so when we upload the files to databricks,
tables will be mostly similar to sql database.
'''
from datetime import datetime
from pathlib import Path
from xmlrpc.client import boolean
from dateutil.parser import *

from pyarrow import csv as pacsv
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import ipdb
import os
import json
import time
from datetime import datetime, timedelta
import csv
import sys
import gc

def get_column_data_type(table_name, column_name):
    schema_path = '/media/zaman/Data Storage/anonymization/seu_schema.sql'
    try:
        with open(schema_path, 'r') as file:
            schema = file.read()
    except Exception as e:
        print(e)
        return False
    else:
        schema_data = schema.split('CREATE TABLE [dbo].')

        target = ''
        for item in schema_data:
            if f'[{table_name}](' in item:
                if f'[{column_name}]' in item:
                    target = item
                    break

        if target:
            data = target.split('\n\t')

            target_item = ''
            for item in data:
                if f'[{column_name}]' in item:
                    target_item = item
                    break
            if target_item:
                data_type = target_item.split(']')[1].replace('[', '').strip()
                return data_type

    return False

def convert_to_datetime(data):
    timestamp = None
    try:
        timestamp = parse(data.strip().replace('"', '').strip())
        timestamp = timestamp.strftime("%m/%d/%Y %H:%M:%S")
        # timestamp = datetime.timestamp(timestamp)
    except Exception as e:
        pass

    return timestamp

def csv_to_parquet_by_arrow(file_dir, dest_dir, check_dir=None):
    dir_list = os.listdir(file_dir)

    count = 0
    success = 0
    match = 0
    error_file = '/home/zaman/repos/csv_to_parquet/error_tables.json'
    print(datetime.now())

    for file in dir_list:
        gc.collect()
        count += 1
        # if count < 1133:
        #     continue
        if check_dir:
            park_file = f"{check_dir}/{file.split('.')[0].rstrip('_')}.parquet"
            file_exists = os.path.isfile(park_file)
            if file_exists:
                # print(f'parquet file found, {count}, {match}, {file}')
                match += 1
                continue
        # file = 'STUD_HOURS_CRS_.csv'
        file_path = Path(file_dir + '/' + file)
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        # if file_size > 1000:
        #     continue
        # if file_size <= 500 or file_size > 1000:
        #     print(count, match, file_size, file)
        #     continue
        print(count, match, file_size, file)
        # continue

        # ipdb.set_trace()
        try:
            parse_options = pacsv.ParseOptions(quote_char='"', newlines_in_values=True)
            csv_data = pacsv.read_csv(file_path, parse_options=parse_options)
        except Exception as e:
            tables = []
            with open(error_file, 'r+') as error_files:
                data = json.load(error_files)
                data['table_names'].append(file)
                error_files.seek(0)
                json.dump(data, error_files, indent=4)

            # ipdb.set_trace()
            print(e)
            continue

        column_names = csv_data.column_names

        schema_list = []
        data_dict = {}
        for item in column_names:
            gc.collect()
            dtype = get_column_data_type(file.split('.')[0].rstrip('_'), item)
            # schema_list.append((item, getattr(pa, dtype)))

            col_data = csv_data.column(item)
            data_list = []
            gc.collect()
            for data in col_data:
                data_list.append(str(data))

            if item in ['APPROWVERSION']:
                data_dict[item] = data_list
                continue
            try:
                if dtype in ['int', 'smallint', 'tinyint', 'bigint', 'bit']:
                    # data = [None if x == 'None' or x == 'False' or x == 'True' else int(x) for x in data_list]
                    data = []
                    for x in data_list:
                        try:
                            data.append(int(x))
                        except Exception as e:
                            if x == 'True':
                                data.append(int(1))
                            elif x == 'False':
                                data.append(int(0))
                            else:
                                data.append(None)
                    data_dict[item] = data
                elif dtype in ['timestamp', 'datetime', 'date', 'time', 'smalldatetime']:
                    data = list(map(convert_to_datetime, data_list))
                    data_dict[item] = data
                elif dtype in ['float', 'decimal', 'numeric']:
                    data = [None if x == 'None' else float(x) for x in data_list]
                    data_dict[item] = data

                else:
                    data_dict[item] = data_list
            except Exception as e:
                ipdb.set_trace()
                print(e)
        # clear memory
        del data
        del data_list
        del csv_data
        gc.collect()

        df = pd.DataFrame(data_dict)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f"{dest_dir}/{file.split('.')[0].rstrip('_')}.parquet")
        success += 1
        print(count, match, success)
        print(datetime.now())
        # break
    return True


def csv_to_parquet(file_dir, dest_dir, check_dir=None):
    dir_list = os.listdir(file_dir)

    count = 0
    success = 0
    match = 0
    error_file = '/home/zaman/repos/csv_to_parquet/error_tables.json'

    for file in dir_list:
        count += 1
        # if count < 1133:
        #     continue
        if check_dir:
            park_file = f"{check_dir}/{file.split('.')[0].rstrip('_')}.parquet"
            file_exists = os.path.isfile(park_file)
            if file_exists:
                # print(f'parquet file found, {count}, {match}, {file}')
                match += 1
                continue

        file_path = Path(file_dir + '/' + file)
        file_size = os.path.getsize(file_path) / (1024 * 1024)

        # if file_size > 1000:
        #     continue

        print(count, match, file_size, file)
        # continue

        # ipdb.set_trace()
        csv.field_size_limit(sys.maxsize)  # maximize field size
        with open(file_path, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            # csv_data = list(reader)
            for row in reader:
                column_names = row.keys()
                break
            print(f'Columns Count : {len(column_names)}')

            csv_data = {}
            for idx, item in enumerate(column_names):
                csv_data[item] = []

            print(f'file reading is started, {len(csv_data)} columns')
            print(datetime.now())
            # ipdb.set_trace()
            for row in reader:
                for item in row:
                    csv_data[item].append(row[item])
            print('file reading is completed')
            print(datetime.now())

            data_dict = {}
            for indx, item in enumerate(csv_data):
                gc.collect()
                dtype = get_column_data_type(file.split('.')[0].rstrip('_'), item)

                # col_data = csv_data[item]
                # data_list = []
                # for data in col_data:
                #     data_list.append(str(data))
                data_list = csv_data[item]

                if item in ['APPROWVERSION']:
                    data_dict[item] = data_list
                    continue
                try:
                    data = []
                    if dtype in ['int', 'smallint', 'tinyint', 'bigint', 'bit']:
                        for x in data_list:
                            try:
                                data.append(int(x))
                            except Exception as e:
                                if x == 'True':
                                    data.append(int(1))
                                elif x == 'False':
                                    data.append(int(0))
                                else:
                                    data.append(None)
                        print(f"{item}, length of data : {len(data)}")
                        data_dict[item] = data
                    elif dtype in ['timestamp', 'datetime', 'date', 'time', 'smalldatetime']:
                        data = list(map(convert_to_datetime, data_list))
                        print(f"{item}, length of data : {len(data)}")
                        data_dict[item] = data
                    elif dtype in ['float', 'decimal', 'numeric']:
                        data = []
                        for x in data_list:
                            try:
                                data.append(float(x))
                            except Exception as e:
                                data.append(None)
                        print(f"{item}, length of data : {len(data)}")
                        data_dict[item] = data

                    else:
                        data_dict[item] = data_list
                        print(f"## {item}, length of data : {len(data)}")
                    print(indx + 1, 'columns convertion done')
                    gc.collect()
                except Exception as e:
                    print(e)
            # clear memory
            del data
            del data_list
            del csv_data
            gc.collect()
            # ipdb.set_trace()
            length = None
            for item in data_dict:
                if length and len(data_dict[item]) != length:
                    ipdb.set_trace()
                    print(len(data_dict))
                length = len(data_dict[item])
            gc.collect()
            print(f'total columns is: {len(data_dict)}')
            df = pd.DataFrame(data_dict)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, f"{dest_dir}/{file.split('.')[0].rstrip('_')}.parquet")
            success += 1
            print(count, match, success, file_size, file)
            print(datetime.now())
        break
    return True


def main():
    csv_dir = '/media/zaman/Data Storage/anonymization/data_anonymization_new/data_big/tarns_hist'
    # csv_dir = '/media/zaman/Data Storage/anonymization/anonymized_22'
    # dest_dir = '/media/zaman/Data Storage/anonymization/parquet_22_new'
    dest_dir = '/media/zaman/Data Storage/anonymization/data_anonymization_new/csv_to_parquet_40/parquet_tables'
    # csv_to_parquet_by_arrow(csv_dir, dest_dir, dest_dir)
    csv_to_parquet(csv_dir, dest_dir, dest_dir)

    return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

