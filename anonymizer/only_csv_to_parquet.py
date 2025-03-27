import csv
import os
import sys
import gc
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dateutil.parser import *
import ipdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp
import shutil
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType, DecimalType
# from anonymizer.anonymizer import get_column_data_type, convert_to_datetime
from decimal import Decimal

class OnlyCsvToParquet():
    def csv_to_parquet_multiple_file(self, file_dir, dest_dir, check_dir=None):
        dir_list = os.listdir(file_dir)

        count = 0
        success = 0
        match = 0
        print("inside csv_to_parquet_multiple_file", dir_list)
        for file in dir_list:
            count += 1
            if check_dir:
                park_file = f"{check_dir}/{file.split('.')[0].rstrip('_')}.parquet"
                file_exists = os.path.isfile(park_file)
                if file_exists:
                    print(f'parquet file found, {count}, {match}, {file}')
                    match += 1
                    continue

            file_path = Path(file_dir + '/' + file)
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            print(count, match, file_size, file)
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

                data_dict = {}
                datetime_column = []
                for indx, item in enumerate(csv_data):
                    gc.collect()
                    dtype = self.get_column_data_type(file.split('.')[0].rstrip('_'), item)

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
                            data = list(map(self.convert_to_datetime, data_list))
                            datetime_column.append(item)
                            print(f"elif dtype {item}, dtype of data : {dtype}")
                            print(data)
                            data_dict[item] = data_list
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
                pq.write_table(table, f"{dest_dir}/{file.split('.')[0].rstrip('_')}.parquet")
                success += 1
                print(count, match, success, file_size, file)
                print(datetime.now())
            # break
        return True
    def csv_to_parquet_by_pyspark(self, file_dir, dest_dir, check_dir=None):
        dir_list = os.listdir(file_dir)

        count = 0
        success = 0
        match = 0
        print("inside csv_to_parquet_multiple_file", dir_list)
        for file in dir_list:
            count += 1
            if check_dir:
                park_file = f"{check_dir}/{file.split('.')[0].rstrip('_')}.parquet"
                file_exists = os.path.isfile(park_file)
                if file_exists:
                    print(f'parquet file found, {count}, {match}, {file}')
                    match += 1
                    continue

            file_path = Path(file_dir + '/' + file)
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            print(count, match, file_size, file)

            file_name = f"{file.split('.')[0].rstrip('_')}.parquet"
            
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

                # ipdb.set_trace()
                for row in reader:
                    for item in row:
                        csv_data[item].append(row[item])
                print('file reading is completed')

                data_dict = {}
                datetime_column = []
                for indx, item in enumerate(csv_data):
                    gc.collect()
                    dtype = self.get_column_data_type(file.split('.')[0].rstrip('_'), item)

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
                            print(f"{item}, length of data : {len(data)} -  {dtype}")
                            data_dict[item] = data
                        elif dtype in ['timestamp', 'datetime', 'date', 'time', 'smalldatetime']:
                            data = list(map(self.convert_to_datetime, data_list))
                            datetime_obj = {
                                'column': item,
                                'type': dtype
                            }
                            datetime_column.append(datetime_obj)
                            print(f"elif dtype {item}, dtype of data : {dtype}")
                            # print(data)
                            data_dict[item] = data_list
                        elif dtype in ['float', 'decimal', 'numeric']:
                            data = []
                            for x in data_list:
                                try:
                                    data.append(float(x))
                                except Exception as e:
                                    data.append(None)
                            print(f"{item}, length of data : {len(data)} - {dtype}")
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
                print(f'total columns is: {len(data_dict)}')
                df = pd.DataFrame(data_dict)
                # print(df.dtypes)
                # ipdb.set_trace()
                self.convert_datetime_by_pyspark(df, dest_dir, file_name, datetime_column)
            # break
        # return True
    def csv_to_parquet_by_pyspark_updated(self, file_dir, dest_dir, check_dir=None):
        dir_list = os.listdir(file_dir)

        count = 0
        success = 0
        match = 0
        print("inside csv_to_parquet_multiple_file", dir_list)
        for file in dir_list:
            count += 1
            if check_dir:
                park_file = f"{check_dir}/{file.split('.')[0].rstrip('_')}.parquet"
                file_exists = os.path.isfile(park_file)
                if file_exists:
                    print(f'parquet file found, {count}, {match}, {file}')
                    match += 1
                    continue

            file_path = Path(file_dir) / file
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            print(count, match, file_size, file)

            file_name = f"{file.split('.')[0].rstrip('_')}.parquet"
            
            csv.field_size_limit(sys.maxsize)  # maximize field size
            with open(file_path, mode='r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)  # Read all rows into a list

            # Convert the data to a Pandas DataFrame
            df = pd.DataFrame(rows)
            print(f'df: {df.dtypes}')

            print(f'Columns Count : {len(df.columns)}')
            print(f'file reading is completed')

            data_dict = {}
            datetime_column = []
            for indx, item in enumerate(df.columns):
                gc.collect()
                dtype = self.get_column_data_type(file.split('.')[0].rstrip('_'), item)
                data_list = df[item].tolist()

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
                        print(f"{item}, length of data : {dtype}")
                        data_dict[item] = data
                    elif dtype in ['timestamp', 'datetime', 'date', 'time', 'smalldatetime']:
                        data = list(map(self.convert_to_datetime, data_list))
                        datetime_obj = {
                            'column': item,
                            'type': dtype
                        }
                        datetime_column.append(datetime_obj)
                        # print(f"elif dtype {item}, dtype of data : {dtype}")
                        # print(data)
                        data_dict[item] = data_list
                    elif dtype in ['float', 'decimal', 'numeric']:
                        for x in data_list:
                            try:
                                data.append(float(x))
                            except Exception as e:
                                data.append(None)
                        print(f"{item}, length of data : {len(data), dtype}")
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
            gc.collect()
            length = None
            for item in data_dict:
                if length and len(data_dict[item]) != length:
                    print(len(data_dict))
                length = len(data_dict[item])
            gc.collect()
            
            print(f'total columns is: {len(data_dict)}')
            # df_dict['ASSET_COST'] = df_dict['ASSET_COST'].fillna(0).astype(int)
            # df_dict['FacilitySpaceAppId'] = df_dict['FacilitySpaceAppId'].fillna(0).astype(int)
            # df_dict['JOB_TIME'] = pd.to_datetime(df_dict['JOB_TIME'])
            default_datetime = '1970-01-01 00:00:00'
            df_dict = pd.DataFrame(data_dict)
            for column in datetime_column:
               df_dict[column['column']] = df_dict[column['column']].fillna(pd.Timestamp('1970-01-01'))
            print('data_dict', data_dict["RETIRE_DTE"])
            print(f'df_dict: {df_dict.dtypes}')
            self.convert_datetime_by_pyspark(df_dict, dest_dir, file_name, datetime_column)
            success += 1
            print(count, match, success, file_size, file)
        return True
    def csv_to_parquet_single_file(self, file_path, dest_dir, check_dir=None):
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
            
            # Read the file while handling NULL bytes
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                data = f.read().replace("\x00", "")  # Remove NULL bytes

            # Write cleaned data back (optional)
            cleaned_csv_file = "cleaned_file.csv"
            with open(cleaned_csv_file, "w", encoding="utf-8") as f:
                f.write(data)

            # Now read with Pandas
            # df = pd.read_csv(cleaned_csv_file)

            with open(cleaned_csv_file, mode='r', newline='') as csvfile:
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
                datetime_column = []
                for indx, item in enumerate(csv_data):
                    gc.collect()
                    dtype = self.get_column_data_type(file_name.split('.')[0].rstrip('_'), item)

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
                        if dtype in ['int', 'smallint', 'tinyint', 'bigint', 'bit']:
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
                            print(f"{item}, length of data : {len(data)}")
                            data_dict[item] = data
                        elif dtype in ['timestamp', 'datetime', 'date', 'time', 'smalldatetime']:
                            # data = list(map(self.convert_to_datetime, data_list))
                            datetime_obj = {
                            'column': item,
                            'type': dtype
                            }
                            datetime_column.append(datetime_obj)
                            data = list(data_list)
                            print(f"elseif{item}, dtype of data : {dtype}")
                            data_dict[item] = data
                            # print(data)
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
                # for column in datetime_column:
                #     df[column['column']] = pd.to_datetime(df[column['column']],format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
                #     # df[column['column']] = df[column['column']].dt.tz_localize(None)
                #     df[column['column']] = df[column['column']].dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
                #     # df[column['column']] = df[column['column']].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                
                # ipdb.set_trace()
                # print(df.dtypes)
                # df.to_parquet(f"{dest_dir}/{file_name.split('.')[0].rstrip('_')}.parquet", engine="pyarrow", index=False)
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
    
    def convert_datetime_by_pyspark(self, df, dest_dir, file_name, datetime_column):
        print('file_name', file_name)
        spark = SparkSession.builder \
            .appName("CSV_to_Parquet") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        # Define custom schema
        # schema = StructType([
        #     StructField(column, StringType(), True) for column in df.columns
        # ])
          # Define schema explicitly
        # print(df.dtypes)
         # Convert datetime strings to datetime objects using Pandas
        for column in datetime_column:
            df[column['column']] = pd.to_datetime(df[column['column']], format='MM/dd/yyyy hh:mm:ss a', errors='coerce')
        # Convert float columns to decimal
        decimal_columns = ["TRANS_AMT", "THIRD_PTY_BILL_AMT", "UNDEFINED_11_2_FLD", "WORK_PERCENT", "TAX_REFUND_AMT", "FOR_CUR_TRANS_AMT", "FOR_CUR_DISCOUNT", "EXCHANGE_RATE"]
        for column in decimal_columns:
            df[column] = df[column].apply(lambda x: Decimal(x) if pd.notnull(x)  else Decimal(0))

         # Convert float columns to integer
        # Convert float columns to integer
        # integer_columns = ["ID_NUM", "CHECK_NUM_NUM", "FEES_SEQUENCE_NUM", "PAYMENT_PLAN_NUM", "GOID"]
        # for column in integer_columns:
        #     df[column] = df[column].apply(lambda x: int(float(x)) if pd.notnull(x) else None)
        df = df.fillna(0).astype({col: 'int64' for col in df.select_dtypes(include=['float64']).columns})
        # Convert integer columns to timestamp
        timestamp_columns = ["APPROWVERSION", "UNDEFINED_DTE_FLD", "PAYABLE_CHECK_DTE", "TRANS_DTE", "JOB_TIME"]
        for column in timestamp_columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')

        df.dtypes.to_csv("dtypes_update2.csv")
        # Replace None values with appropriate defaults

        schema = StructType([
            StructField("APPID", IntegerType(), False),
            StructField("SOURCE_CDE", StringType(), False),
            StructField("GROUP_NUM", IntegerType(), False),
            StructField("TRANS_KEY_LINE_NUM", IntegerType(), False),
            StructField("TRANS_DTE", TimestampType(), True),
            StructField("TRANS_AMT", DecimalType(11, 2), False),
            StructField("TRANS_DESC", StringType(), True),
            StructField("FOLIO", StringType(), True),
            StructField("ACCT_CDE", StringType(), True),
            StructField("GL_MASTER_APPID", IntegerType(), True),
            StructField("PROJECT_CODE", StringType(), True),
            StructField("ENCUMB_GL_FLAG", StringType(), False),
            StructField("ENCUMB_GL_TRANS_ST", StringType(), False),
            StructField("AP_SBS_ID_NUM", IntegerType(), True),
            StructField("AP_SBS_CDE_SUBSID", StringType(), True),
            StructField("INVOICE_NUM", StringType(), True),
            StructField("ID_NUM", IntegerType(), True),
            StructField("SUBSID_CDE", StringType(), True),
            StructField("OFFSET_FLAG", StringType(), True),
            StructField("SUBSID_TRANS_STS", StringType(), False),
            StructField("PAYABLE_CHECK_DTE", TimestampType(), True),
            StructField("CHECK_NUM_ALPHA", StringType(), True),
            StructField("CHECK_NUM_NUM", IntegerType(), True),
            StructField("TRANS_PO_NUM_GRP_N", IntegerType(), True),
            StructField("PO_LINE_NUM", IntegerType(), True),
            StructField("DISCOUNT", IntegerType(), False),
            StructField("RECEIPT_NUM", StringType(), True),
            StructField("ABA_NUM", StringType(), True),
            StructField("AR_CDE", StringType(), True),
            StructField("GL_SEL_FLAG", StringType(), True),
            StructField("PARTIAL_ORDER_PO", StringType(), True),
            StructField("DISCOUNT_TAKEN", StringType(), True),
            StructField("THIRD_PTY_BILL_ID", IntegerType(), True),
            StructField("THIRD_PTY_BILL_AMT", DecimalType(11, 2), False),
            StructField("OPEN_ITEMS_INV_NUM", StringType(), True),
            StructField("CHG_FEE_CDE", StringType(), True),
            StructField("FEES_SEQUENCE_NUM", IntegerType(), True),
            StructField("OI_SOURCE_CDE", StringType(), True),
            StructField("OI_GROUP_NUM", IntegerType(), True),
            StructField("OI_TRANS_KEY_LINE_NUM", IntegerType(), True),
            StructField("OI_TRANS_HIST_APPID", IntegerType(), True),
            StructField("OI_ALLOCATION", StringType(), False),
            StructField("FAP_CDE", StringType(), True),
            StructField("FUND_CDE", IntegerType(), True),
            StructField("UNDEFINED_DTE_FLD", TimestampType(), True),
            StructField("UNDEFINED_1A_FLD_1", StringType(), True),
            StructField("UNDEFINED_1A_FLD_2", StringType(), True),
            StructField("UNDEFINED_10A_FLD", StringType(), True),
            StructField("UNDEFINED_11_2_FLD", DecimalType(11, 2), False),
            StructField("WORK_PERCENT", DecimalType(5, 2), False),
            StructField("TAX_REFUND_CDE", StringType(), True),
            StructField("TAX_REFUND_AMT", DecimalType(11, 2), False),
            StructField("FOR_CUR_TRANS_AMT", DecimalType(11, 2), False),
            StructField("FOR_CUR_DISCOUNT", DecimalType(11, 2), False),
            StructField("CURR_CODE", StringType(), True),
            StructField("EXCHANGE_RATE", DecimalType(14, 7), False),
            StructField("PAYMENT_PLAN_CDE", StringType(), True),
            StructField("PAY_PLAN_SEQ_NUM", IntegerType(), True),
            StructField("AR_INVOICE_NUM", StringType(), True),
            StructField("ELIG_1098T", StringType(), True),
            StructField("CREDIT_MEMO", StringType(), True),
            StructField("ASSET_NUMBER", StringType(), True),
            StructField("CHG_TRM_TRAN_HIST", StringType(), True),
            StructField("CHG_YR_TRAN_HIST", StringType(), True),
            StructField("GOVT_FORM_CODE", StringType(), True),
            StructField("GOVT_LABEL_CODE", StringType(), True),
            StructField("PAYMENT_PLAN_NUM", IntegerType(), True),
            StructField("GOID", IntegerType(), True),
            StructField("BILLING_PERIOD_ID", IntegerType(), True),
            StructField("IS_PAID", StringType(), True),
            StructField("APPROWVERSION", TimestampType(), False),
            StructField("USER_NAME", StringType(), True),
            StructField("JOB_NAME", StringType(), True),
            StructField("JOB_TIME", TimestampType(), True)
        ])
       
        # Convert Pandas DataFrame to PySpark DataFrame
        spark_df = spark.createDataFrame(df, schema=schema)
        # Print schema to verify column names
        print("schema", spark_df.printSchema())
        # Convert columns explicitly
        date_format_str = 'MM/dd/yyyy hh:mm:ss a'
        print("datetime_column", datetime_column)
        default_datetime = '1970-01-01 00:00:00'
        # for column in datetime_column:
        #     spark_df = spark_df.withColumn(column['column'], spark_df[column['column']].cast(TimestampType()))
        #     spark_df = spark_df.fillna({column['column']: default_datetime})
        # for column in datetime_column:
        #     spark_df = spark_df.withColumn(column['column'], to_timestamp(spark_df[column['column']], 'MM/dd/yyyy hh:mm:ss a'))
        #     spark_df = spark_df.fillna({column['column']: default_datetime})
            
        for column in datetime_column:
            if column['type'] == 'datetime' or column['type'] == 'timestamp':
                spark_df = spark_df.withColumn(column['column'], to_timestamp(spark_df[column['column']], date_format_str))
            else:
                print("else")
                spark_df = spark_df.withColumn(column['column'], to_date(spark_df[column['column']], 'MM/dd/yyyy'))
        # Convert float_col1 to integer
        # spark_df = spark_df.withColumn("FacilitySpaceAppId", df["FacilitySpaceAppId"].cast(IntegerType()))
        
        # Save as Parquet
        # spark_df.coalesce(1).write.mode("overwrite").parquet(dest_dir)

        # Save as a single Parquet file
        temp_dir = Path(dest_dir) / "temp_parquet"
        spark_df.coalesce(1).write.mode("overwrite").parquet(str(temp_dir))

        # Move the part file to the desired file name
        part_file = next(temp_dir.glob("part-*.parquet"))
        part_file.rename(Path(dest_dir) / file_name)

        # Remove the temporary directory
        shutil.rmtree(temp_dir)
        
        # Stop the Spark session
        spark.stop()
            
    def convert_to_datetime(self, data):
        timestamp = None
        try:
            timestamp = parse(data.strip().replace('"', '').strip())
        except Exception as e:
            pass

        return timestamp
    
    def convert_to_datetime_single_file(self, data):
        timestamp = None
        try:
            timestamp = parse(data.strip().replace('"', '').strip())
            timestamp = timestamp.strftime("%m/%d/%Y %H:%M:%S")
            timestamp = datetime.timestamp(timestamp)
        except Exception as e:
            pass

        return timestamp

def main():
    csv_dir = '/media/zaman/Data Storage/anonymization/data_anonymization_new/data_big/tarns_hist/3_6'
    csv_file_path = '/media/zaman/Data Storage/anonymization/data_anonymization_new/data_big/tarns_hist/3_6/2_TRANS_HIST_.csv'
    # csv_dir = '/media/zaman/Data Storage/anonymization/anonymized_22'
    # dest_dir = '/media/zaman/Data Storage/anonymization/parquet_22_new'
    dest_dir = '/media/zaman/Data Storage/anonymization/data_anonymization_new/csv_to_parquet_40/parquet_tables'
    # csv_to_parquet_by_arrow(csv_dir, dest_dir, dest_dir)
    csv_to_parquet = OnlyCsvToParquet()
    csv_to_parquet.csv_to_parquet_single_file(csv_file_path, dest_dir, dest_dir)
    # csv_to_parquet.csv_to_parquet_multiple_file(csv_dir, dest_dir, dest_dir)
    # csv_to_parquet.csv_to_parquet_by_pyspark_updated(csv_dir, dest_dir, dest_dir)
    # csv_to_parquet.csv_to_parquet_by_pyspark(csv_dir, dest_dir, dest_dir)

    return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

