import matplotlib.pyplot as plt



if __name__ == "__main__":

    import argparse
    import pandas as pd
    from os.path import dirname, realpath
    import constants as consts

    from smartflux_reader import verify_path_isdir, process_smartflux_data

    # default path is here
    default_dir=verify_path_isdir(f"{dirname(realpath(__file__)):s}")

    # default path is here
    parser = argparse.ArgumentParser()
    parser.add_argument('-id','--input_dir', required=True, type=str,
        help='path to smartflux original data directory')
    parser.add_argument('-od', '--output_dir', default=default_dir, type=str,
        help='path to processed (output) data directory')
    args = parser.parse_args()
    argdict = vars(args)

    # process data (locate .ghg files, unzip, merge, and return pd dataframe)
    processed_data = process_smartflux_data(argdict['input_dir'], argdict['output_dir'])
    pd.set_option('display.precision', 2)
    print("\nNumber of Samples:", len(processed_data))
    print("Column Names:", ', '.join([str(c) for c in processed_data.columns]), end="\n\n")

    m_air = consts.MM_AIR * 1E-3       # kg/mol
    m_ch4 = consts.MM_CH4 * 1E-3       # kg/mol
    m_co2 = consts.MM_CO2 * 1E-3       # kg/mol
    m_h2o = consts.MM_H2O * 1E-3       # kg/mol

    P0 = processed_data['CH4 Pressure'] * 1E3     # Pa
    T0 = processed_data['CH4 Temperature'] + 273.15 # K
    rho0  = (m_air / consts.R) * (P0 / T0)  # kg/m^3
    processed_data["Density (kg/m^3)"] = rho0

    rho_df = processed_data[['Timestamp', 'Density (kg/m^3)','Pressure (kPa)', 'Temperature (C)']]

    P1 = processed_data['Pressure (kPa)'] * 1E3     # Pa
    T1 = processed_data['Temperature (C)'] + 273.15 # K
    rho1  = (m_air / consts.R) * (P1 / T1)  # kg/m^3

    # todo: check units...
    ch4_ppm = (processed_data["CH4 (mmol/m^3)"] * 1E3) * (m_ch4 / rho0) # (umol/m^3) * (kg/mol) / (kg/m^3) = mol/mol
    co2_ppm = (processed_data["CO2 (mmol/m^3)"] * 1E3) * (m_co2 / rho0) # (umol/m^3) * (kg/mol) / (kg/m^3) = mol/mol
    h2o_ppm = (processed_data["H2O (mmol/m^3)"] * 1E3) * (m_h2o / rho0) # (umol/m^3) * (kg/mol) / (kg/m^3) = mol/mol

    fig = plt.figure()
    fig.set_figheight(8)
    fig.set_figwidth(14)

    ax = fig.add_subplot(111)

    #ax.plot()



    fig.show()