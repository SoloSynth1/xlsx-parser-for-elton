import argparse
from multiprocessing import Pool

import pandas as pd


HEADERS = ['VENDOR', "Code", 'IsSelected', 'IsSelectedEP']


def initialize(export_filename):
    with open(export_filename, 'w') as f:
        f.write(",".join(HEADERS) + "\n")


def parse_record(df, i):
    data_columns = [x for x in df.columns if 'Vendor' not in x]
    non_ep_columns = [x for x in data_columns if "EP" not in x]
    ep_columns = [x for x in data_columns if "EP" in x]
    col_0 = [df.iloc[i]['Vendor'] for _ in range(len(non_ep_columns))]
    col_1 = non_ep_columns
    col_2 = [True if df.iloc[i][column] == "X" else False for column in non_ep_columns]
    col_3 = [True if df.iloc[i][column] == "X" else False for column in ep_columns]
    new_df = pd.DataFrame([col_0, col_1, col_2, col_3]).T
    return new_df


def write_record(df, i, export_filename):
    parse_record(df, i).to_csv(export_filename, index=None, header=None, mode='a')


def main(excel_path, export_filename, threads):
    initialize(export_filename)
    df = pd.read_excel(excel_path)
    p = Pool(threads)
    p.starmap(write_record, [(df, i, export_filename) for i in range(len(df))])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str, help='input file (in XLSX format)')
    parser.add_argument('output', type=str, help='output file (in CSV format)')
    parser.add_argument('-t', '--threads', type=int, default=8, help='')

    args = parser.parse_args()
    excel_path = args.input
    export_filename = args.output
    threads = args.threads

    main(excel_path, export_filename, threads)
