import csv
import os
import sys
import gc
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
from pathlib import Path
from dateutil.parser import *
import ipdb
# from anonymizer.anonymizer import get_column_data_type, convert_to_datetime


class CsvToParquet():
    
    def csv_to_parquet(self, file_path, dest_dir, check_dir=None):
        # dir_list = os.listdir(file_dir)
        file_name = os.path.basename(file_path)

        count = 0
        success = 0
        match = 0
        error_file = '/home/sgc/test anonymize/error_tables.json'

        # for file in dir_list:
        count += 1
        # if count < 1133:
        #     continue
        file_exists = False
        if check_dir:
            park_file = f"{check_dir}/{file_name.split('.')[0].rstrip('_')}.parquet"
            is_file_exists = os.path.isfile(park_file)
            if is_file_exists:
                file_exists = True
                # print(f'parquet file found, {count}, {match}, {file}')
                match += 1
                # continue

        # file_path = Path(file_dir + '/' + file)
        file_size = os.path.getsize(file_path) / (1024 * 1024)

        # if file_size > 1000:
        #     continue

        print(count, match, file_size, file_path)
        # continue

        # ipdb.set_trace()
        if file_exists is not True: 
            csv.field_size_limit(sys.maxsize)  # maximize field size
            with open(file_path, mode='r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                print('file reading is started', reader)
                # csv_data_list = list(reader)
                # for row in reader:
                #     column_names = row.keys()
                #     break
                column_names = reader.fieldnames
                # ipdb.set_trace()

                csv_data = {}
                for idx, item in enumerate(column_names):
                    csv_data[item] = []

                for row in reader:
                    for item in row:
                        csv_data[item].append(row[item])
                print('file reading is completed')

                data_dict = {}
                # ipdb.set_trace()
                for indx, item in enumerate(csv_data):
                    gc.collect()
                    dtype = self.get_column_data_type(file_name.split('.')[0].rstrip('_'), item)
                    print('file_name', file_name.split('.')[0].rstrip('_'))
                    # ipdb.set_trace()
                    # col_data = csv_data[item]
                    # data_list = []
                    # for data in col_data:
                    #     data_list.append(str(data))
                    data_list = csv_data[item]
                    data = []

                    if item in ['APPROWVERSION']:
                        data_dict[item] = data_list
                        continue
                    try:
                        print('dtype', dtype)
                        if dtype in ['int', 'smallint', 'tinyint', 'bigint', 'bit']:
                            # data = []
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
                            print(f"if {item}, length of data : {len(data)}")
                            data_dict[item] = data
                        elif dtype in ['timestamp', 'datetime', 'date', 'time', 'smalldatetime']:
                            data = list(map(self.convert_to_datetime, data_list))
                            print(f"elif date{item}, length of data : {len(data)}")
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
                        # ipdb.set_trace()
                        print(len(data_dict))
                    length = len(data_dict[item])
                gc.collect()
                print(f'total columns is: {len(data_dict)}')
                df = pd.DataFrame(data_dict)
                table = pa.Table.from_pandas(df)
                pq.write_table(table, f"{dest_dir}/{file_name.split('.')[0].rstrip('_')}.parquet")
                success += 1
                print(count, match, success, file_size, file_name)
        else:
            print(f'parquet file found, {count}, {match}, {file_name}')
        return True

    def get_column_data_type(self, table_name, column_name):
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
                # ipdb.set_trace()
                target_item = ''
                for item in data:
                    if f'[{column_name}]' in item:
                        target_item = item
                        break
                if target_item:
                    data_type = target_item.split(']')[1].replace('[', '').strip()
                    return data_type

        return False

    def convert_to_datetime(self, data):
        timestamp = None
        try:
            timestamp = parse(data.strip().replace('"', '').strip())
            timestamp = timestamp.strftime("%m/%d/%Y %H:%M:%S")
            timestamp = datetime.timestamp(timestamp)
        except Exception as e:
            pass

        return timestamp
