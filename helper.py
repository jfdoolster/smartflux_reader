import os
import pandas as pd

def df2pkl(dataframe: pd.DataFrame, pkl_path: str, compression='infer'):
    """
    save dataframe to pkl
    """
    dataframe.to_pickle(unix_path(pkl_path), compression=compression)
    print(f"created pkl file: {unix_path(pkl_path)}")

def df2csv(dataframe: pd.DataFrame, csv_path: str, index=False):
    """
    save dataframe to csv
    """
    dataframe.to_csv(csv_path, index=index)

def unix_path(filepath):
    """
    return unix-style filepath
    """
    return os.path.normpath(filepath).replace(os.sep, '/')
