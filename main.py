if __name__ == "__main__":

    from os.path import dirname, realpath
    import argparse

    from licor_reader import verify_path_isdir, process_licor_data

    default_dir=verify_path_isdir(f"{dirname(realpath(__file__)):s}")

    parser = argparse.ArgumentParser()
    parser.add_argument('-id','--input_dir', required=True, type=str,
        help='path to smartflux original data directory')
    parser.add_argument('-od', '--output_dir', default=default_dir, type=str,
        help='path to processed (output) data directory')
    args = parser.parse_args()
    argdict = vars(args)

    processed_data = process_licor_data(argdict['input_dir'], argdict['output_dir'])
