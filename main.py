import csv
from datetime import datetime
import logging


def read_data(file_path):
    ''' read csv file and rows based on header leangth. Return fitting and missformatted data'''
    with open(file_path) as f:
        reader = csv.reader(f, delimiter=',')
        data = []
        missformatted_data = []
        for i, row in enumerate(reader):
            if i == 0:
                header_len = len(row)
                data.append(row)
                missformatted_data.append(row)
            else:
                if len(row) == header_len:
                    data.append(row)
                else:
                    missformatted_data.append(row)
    return data, missformatted_data


def filter_data(data, col_name, value):
    ''' select rows that has specified collumn value '''
    filtered_data = []
    filtered_data.append(data[0])
    column_id = data[0].index(col_name)
    for row in data[1:]:
        if row[column_id] == value:
            filtered_data.append(row)
    return filtered_data


def join_columns(data, col_name_1, col_name_2, delimiter):
    ''' join values from two columns '''
    col_id_1 = data[0].index(col_name_1)
    col_id_2 = data[0].index(col_name_2)
    new_col = []
    for row in data[1:]:
        joined_val = delimiter.join([str(row[col_id_1]), str(row[col_id_2])])
        new_col.append(joined_val)
    return new_col


def add_column(data, col_name, col_values):
    ''' add new column values to the end of the rows '''
    data = data.copy()
    data[0].append(col_name)
    for row_id in range(len(data) - 1):
        data[row_id + 1].append(col_values[row_id])
    return data


def format_names(data):
    ''' join first and last name '''
    col_name_1 = 'CandidateFirstName'
    col_name_2 = 'CandidateLastName'
    delimiter = ' '
    joined_col_name = 'CandidateName'
    joined_values = join_columns(data, col_name_1, col_name_2, delimiter)
    data = add_column(data, joined_col_name, joined_values)
    return data


def convert_to_iso(data, col_name):
    ''' convert date to iso format, write None if cannot convert '''
    col_id = data[0].index(col_name)
    new_values = []
    for row in data[1:]:
        try:
            new_values.append(datetime.strptime(row[col_id], '%m/%d/%Y'))
        except Exception as e:
            new_values.append(None)
    return new_values


def format_dates(data):
    ''' converted dates to ISO format and add to data '''
    col_names = ['PeriodBegining', 'PeriodEnding']
    for col_name in col_names:
        converted_values = convert_to_iso(data, col_name)
        new_col_name = col_name + 'Iso'
        data = add_column(data, new_col_name, converted_values)
    return data


def find_col_ids_by_names(header, col_names):
    ''' find column ids in the header '''
    col_ids = [header.index(col_name) for col_name in col_names]
    return col_ids


def select_cols(data, col_names):
    ''' select columns from data'''
    selected_data = []
    col_ids = find_col_ids_by_names(data[0], col_names)
    selected_data.append([data[0][i] for i in col_ids])
    for row in data[1:]:
        selected_data.append([row[i] for i in col_ids])
    return selected_data


def remove_none(data, col_names):
    ''' split data into rows that contain None and those that do not '''
    clean_data = []
    rows_with_none = []
    clean_data.append(data[0])
    rows_with_none.append(data[0])
    col_ids = find_col_ids_by_names(data[0], col_names)
    for row in data[1:]:
        selected_values = [row[i] for i in col_ids]
        if None in selected_values:
            rows_with_none.append(row)
        else:
            clean_data.append(row)
    return clean_data, rows_with_none


def write_to_csv(data, file_name):
    with open(file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)


def process_data(file_path, output_path, misformatted_output_path):
    ''' read, clean, format ,and save data '''
    data, misformatted = read_data(file_path)
    formatted_data = filter_data(data, 'CandidateOrCommittee', 'COH')
    formatted_data = format_names(formatted_data)
    formatted_data = format_dates(formatted_data)
    clean_data, no_dates_data = remove_none(
        formatted_data, ['PeriodBeginingIso', 'PeriodEndingIso'])
    no_dates_data = select_cols(no_dates_data, data[0])
    misformatted += no_dates_data[1:]
    col_names = ['CandidateName', 'PeriodBeginingIso', 'PeriodEndingIso',
                 'TransactionID', 'TransactionType', 'TransactionAmount']
    selected_data = select_cols(clean_data, col_names)

    write_to_csv(selected_data, output_path)
    write_to_csv(misformatted, misformatted_output_path)


if __name__ == '__main__':
    file_path = 'data/transactions.csv'
    output_path = 'data/processed_transactions.csv'
    misformatted_output_path = 'data/error.log'
    process_data(file_path, output_path, misformatted_output_path)
