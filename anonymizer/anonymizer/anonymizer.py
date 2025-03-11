import random
from config import config
import random
from datetime import datetime, timedelta
from fake_email import Email
from faker import Faker
import ipdb
import csv
import os

def get_random_integer(start, end, is_string=False):
    if is_string:
        return str(random.randint(start, end))
    return random.randint(start, end)

def year_code_anonymizer():
    return get_random_integer(2000, 2020, is_string=True)

def address_anonymizer(value):
    if value:
        fake = Faker()
        return fake.address().replace('\n', ' ').split(',')[0]
        # return fake.address().split('\n')[0]
    else:
        return None

def state_anonymizer(value):
    if value:
        fake = Faker()
        return fake.state()
    else:
        return None

def email_anonymizer(value):
    if value:
        data = value.split('@')
        fake = Faker()
        name = fake.name().split(' ')[0]
        email = name + '@' + data[-1]
        return email
        # try:
        #     mail=Email().Mail()
        #     return mail['mail']
        # except Exception as e:
        #     ipdb.set_trace()
        #     print(e)
    else:
        return None

def name_anonymizer(value):
    if value:
        fake = Faker()
        name = fake.name().split(' ')[0].replace('.','')
        return name
    else:
        return None

def full_name_anonymizer(value):
    if value:
        fake = Faker()
        name = fake.name()
        return name
    else:
        return None

def number_anonymizer(value):
    if value:
        l = len(value)
        if isinstance(value, str) :
            return str(random.randint(10**(l-1), (10**l)-1))
        return random.randint(10**(l-1), (10**l)-1)
    else:
        return None

def datetime_anonymizer(value):
    if not value:
        return None
    fake = Faker()
    # returns 3/17/2006 12:00:00 AM
    year = get_random_integer(2000, 2020)
    
    # Generate a random day in the year
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    
    # Generate a random time
    random_time = timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    
    # Combine date and time
    random_datetime = random_date + random_time
    
    # Format as MM/DD/YYYY HH:MM:SS AM/PM
    formatted_datetime = random_datetime.strftime("%m/%d/%Y %I:%M:%S %p")
    
    return formatted_datetime

def get_column(column, data):
    return [row[column] for row in data]

class Anonymizer:
    def __init__(self):
        self.config = config

    def anonymize(self, file, data):
        anonymizers = self.config['anonymize_columns_d2l']
        anonymized_data = []
        count = 0
        for row in data:
            count += 1
            anonymized_row = {}
            for key, value in row.items():
                if value and key in anonymizers:
                    if anonymizers[key] == 'random_number':
                        anonymized_row[key] = number_anonymizer(value)
                    elif anonymizers[key] == 'random_name':
                        anonymized_row[key] = name_anonymizer(value)
                    elif anonymizers[key] == 'random_full_name':
                        anonymized_row[key] = full_name_anonymizer(value)
                    elif anonymizers[key] == 'random_email':
                        anonymized_row[key] = email_anonymizer(value)
                    elif anonymizers[key] == 'random_address':
                        anonymized_row[key] = address_anonymizer(value)
                    elif anonymizers[key] == 'random_date':
                        anonymized_row[key] = datetime_anonymizer(value)
                    else:
                        print('invalid key')
                else:
                    anonymized_row[key] = value
            anonymized_data.append(anonymized_row)
            if count % 1000 == 0:
                print(f'anonymized {count}')
        return anonymized_data
    
    def anonymize_and_save(self, file_path, data):
        anonymizers = self.config['anonymize_columns_j1_seu']

        file_exists = os.path.isfile(file_path)
        headers = data[0].keys()

        if file_exists:
            csvfile = open(file_path, mode='a', newline='', encoding='utf-8')
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            print('file exists')
        else:
            csvfile = open(file_path, mode='w', newline='', encoding='utf-8')
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            # Write the header
            writer.writeheader()
            print('header written')

        for indx, row in enumerate(data):
            # if indx < 258931:
            #     continue
            anonymized_row = {}
            for key, value in row.items():
                if value and key in anonymizers:
                    if anonymizers[key] == 'random_number':
                        anonymized_row[key] = number_anonymizer(value)
                    elif anonymizers[key] == 'random_name':
                        anonymized_row[key] = name_anonymizer(value)
                    elif anonymizers[key] == 'random_full_name':
                        anonymized_row[key] = full_name_anonymizer(value)
                    elif anonymizers[key] == 'random_email':
                        anonymized_row[key] = email_anonymizer(value)
                    elif anonymizers[key] == 'random_address':
                        anonymized_row[key] = address_anonymizer(value)
                    elif anonymizers[key] == 'random_date':
                        anonymized_row[key] = datetime_anonymizer(value)
                    # elif anonymizers[key] == 'random_state':
                    #     anonymized_row[key] = state_anonymizer(value)
                    else:
                        print('invalid key')
                else:
                    anonymized_row[key] = value
            writer.writerow(anonymized_row)
            if indx > 1000000:
                break
            if indx % 1000 == 0:
                print(f'anonymized1 {indx}')
        # csvfile.close()
        return True
