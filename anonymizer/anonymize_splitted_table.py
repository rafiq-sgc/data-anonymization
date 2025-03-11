import click
from pathlib import Path
from csv_to_parquet import CsvToParquet
from readers.csv import read_csv_with_error, read_csv, split_large_csv_to_multiple
from anonymizer.anonymizer import Anonymizer
from rich import print
from writers.csv_writer import write_dicts_to_csv, write_dicts_to_multiple_csv
from config import config
import pandas as pd
import ipdb
import os
import time


def check_files(filenames):
    """Check if files exist."""
    input_dir = Path(config['input_dir'])
    existing_files = []
    for fname in filenames:
        file = input_dir/fname
        if file.exists():
            existing_files.append(file)
        else:
            print(f"File {file} does not exist.")

    return existing_files

def data_anonymization(file_dir, out_dir, check_dir=None):
    dir_list = os.listdir(file_dir)
    print('dir_list', len(dir_list), dir_list)

    count = 0
    success = 0
    match = 0

    for file in dir_list:
        count += 1

        if check_dir:
            file_exists = os.path.isfile(check_dir/file)
            if file_exists:
                print(f'file anonymized, {count}, {file}')
                # match += 1
                continue
            else:
                print('else',count, file)

        file_path = file_dir + '/' + file
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        print('file size', file_size, file)

        data = read_csv(file_path)
        out_file = Path(out_dir) / file

        if data:
            # Shorten code for the below block
            anonymize = any(key in config['anonymize_columns_j1_seu'] for key in data[0].keys())

            if anonymize:
                anonymizer = Anonymizer()
                is_anonymized = anonymizer.anonymize_and_save(out_file, data)
                print('out_file', out_file)
                # After anonymization, convert csv to parquet
                if is_anonymized:
                    csv_to_parquet = CsvToParquet()
                    dest_dir = Path('/media/zaman/Data Storage/anonymization/data_anonymization_new/csv_to_parquet_40/parquet_5_tables')
                    csv_to_parquet.csv_to_parquet(out_file, dest_dir, dest_dir)
            else:
                write_dicts_to_csv(out_file, data)

            success += 1
        print(f'success {success} of {count}')
        # time.sleep(60)
        # break
    print(count, match)
    return True


@click.command()
@click.argument('filenames', nargs=-1)
def main(filenames):
    response_file_dir = '/home/zaman/Downloads/responses'
    # column_list = parse_openai_responses(response_file_dir)
    # ipdb.set_trace()


    file_path = '/media/zaman/Data Storage/anonymization/data_big/transaction_hist'
    save_dir = '/home/zaman/Downloads/anonymization/seu/split_data'
    # split_file = split_large_csv_file(file_path, save_dir)

    file_dir = '/media/zaman/Data Storage/anonymization/data_anonymization_new/big_table2' # running all for this directory, 7 big file are remaining
    out_dir = Path('/media/zaman/Data Storage/anonymization/data_anonymization_new/anonymized_rest_tables')
    # check_dir = Path('/home/zaman/Downloads/anonymization/seu/anonymized_1')
    check_dir = Path('/media/zaman/Data Storage/anonymization/data_anonymization_new/anonymized_rest_tables')
    anonymized = data_anonymization(file_dir, out_dir, check_dir)


if __name__ == '__main__':
    main()
