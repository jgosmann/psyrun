import pandas as pd


def save_infile(df, infile):
    return df.to_hdf(infile, 'pspace')


def load_infile(infile):
    return pd.read_hdf(infile, 'pspace')


def save_outfile(df, outfile):
    return df.to_hdf(outfile, 'results')


def load_results(filename):
    return pd.read_hdf(filename, 'results')
