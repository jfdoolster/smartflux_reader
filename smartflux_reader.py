import os
import sys
import glob
import zipfile
from io import StringIO
import pandas as pd
from tqdm import tqdm
from helper import unix_path, df2pkl, df2csv

def verify_path_isdir(_path):
    _path = unix_path(_path)
    if not os.path.exists(_path):
        print(f"ERROR:\t{_path:s} does not exist")
        sys.exit()
    if not os.path.isdir(_path):
        print(f"ERROR:\t{_path:s} is not a directory")
        sys.exit()

    last = _path[-1]
    while last == '/':
        _path = _path[:-1]
        last = _path[-1]

    return _path

def verify_path_isfile(_path) -> bool:
    _path = unix_path(_path)
    if not os.path.exists(_path):
        #print(f"INFO:\t{_path:s} does not exist")
        return False
    if not os.path.isfile(_path):
        #print(f"INFO:\t{_path:s} is not a directory")
        return False
    print(f"INFO:\tfound processed data file ({_path:s})")
    return True

def process_smartflux_data(data_dir: str, processed_dir: str, processed_ftype="pkl", \
                            processed_fname="processed_licor_data", \
                                raw_fname="raw_licor_data"):

    if processed_ftype.lower() not in ['pkl', 'csv', '.pkl', '.csv']:
        print("ERROR:\tprocessed_file_type must be 'csv' or 'pkl'")

    verify_path_isdir(data_dir)

    if verify_path_isfile(f"{processed_dir:s}/{processed_fname:s}.pkl") or \
        verify_path_isfile(f"{processed_dir:s}/{processed_fname:s}.csv"):
        res = input("regenerate processed files? [N]/y ")
        if res.lower() not in ['y', 'yes']:
            print("reading processed dataframe...")
            if 'pkl' in processed_ftype.lower():
                return pd.read_pickle(f"{processed_dir:s}/{processed_fname:s}.pkl")
            if 'csv' in processed_ftype.lower():
                return pd.read_csv(f"{processed_dir:s}/{processed_fname:s}.csv")

    print("reading processed dataframe...")
    final  = generate_datafile(f'{data_dir:s}/raw', processed_dir, raw_fname)

    print("saving processed dataframe...")
    if 'pkl' in processed_ftype.lower():
        df2pkl(final, f"{processed_dir:s}/{processed_fname:s}.pkl")
    if 'csv' in processed_ftype.lower():
        df2csv(final, f"{processed_dir:s}/{processed_fname:s}.csv")

    return final

def generate_datafile(raw_data_dir: str, output_data_dir: str, raw_fname: str) -> pd.DataFrame:
    raw_data_dir = verify_path_isdir(raw_data_dir)

    # to store files in a list
    df_list = []

    # dirs=directories
    counter = 0
    ghg_file_list = glob.glob(f"{raw_data_dir:s}/*.ghg")
    pbar = tqdm(range(len(ghg_file_list)))
    cols = []
    print("unzipping raw licor data...")
    for i in pbar:
        fname = unix_path(ghg_file_list[i])
        pbar.set_description(f"Processing {fname:s}")
        with zipfile.ZipFile(f"{fname:s}",'r') as zip_ref:
            for zf in zip_ref.namelist():
                if '.data' in zf:
                    if counter == 0:
                        zip_ref.extract(zf, f"{output_data_dir}")
                        base = os.path.basename(zf).split('.')[0]
                        os.rename(f"{output_data_dir}/{base:s}.data", f"{output_data_dir}/{raw_fname:s}.csv")
                        counter+=1
                    with zip_ref.open(zf) as datafile:
                        tsv_string = datafile.read().decode("utf-8")
                        hdrs = tsv_string[:1024].split('\n')
                        for hdr in hdrs:
                            shdr = hdr.split('\t')
                            if shdr[0] == "DATAH":
                                cols = shdr
                            elif shdr[0] == "DATA":
                                break
                        tmp = pd.read_csv(StringIO(tsv_string),sep='\t',skiprows=8,
                                            header=None, low_memory=False)
                        for i,col in enumerate(cols):
                            tmp = tmp.rename(columns={tmp.columns[i]: col})
                        df_list.append(tmp)

    print("creating processed dataframe...")
    final = pd.concat(df_list)
    final['Timestamp'] = pd.to_datetime(final['Date'] + ' ' + final['Time'], format='%Y-%m-%d %H:%M:%S:%f')

    #todo: figure this out!!!!
    final = final.rename(columns={'CH4 (mmol/m^3)':'CH4 (ppm?)',
                            'CH4 Temperature':'CH4 (mmol/m^3)',
                            'CH4 Pressure':'CH4 Temperature',
                            'CH4 Signal Strength':'CH4 Pressure',
                            'CH4 Diagnostic Value':'CH4 Signal Strength',
                            'CHK': 'CH4 Diagnostic Value',
                            })


    return final

if __name__ == "__main__":

    from os.path import dirname, realpath
    import argparse

    default_dir=verify_path_isdir(f"{dirname(realpath(__file__)):s}")

    parser = argparse.ArgumentParser()
    parser.add_argument('-id','--input_dir', required=True, type=str,
        help='path to smartflux data directory')
    parser.add_argument('-od', '--output_dir', default=default_dir, type=str,
        help='path to processed (output) data directory')
    args = parser.parse_args()
    argdict = vars(args)

    processed_data = process_smartflux_data(argdict['input_dir'], argdict['output_dir'])
    pd.set_option('display.precision', 2)
    print(processed_data.columns)
    print(processed_data)

