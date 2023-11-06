# conda activate CAMS_Europe

# Libreries
import os
from datetime import datetime, timedelta
import xarray as xr
import warnings
from datetime import datetime, timedelta
import argparse

warnings.filterwarnings("ignore")

# Path of CAMS Europe
path_main_dir_CAMS_Europe_data = os.environ['CAMS_Europe']

if path_main_dir_CAMS_Europe_data == "":
    print("Error: set the environmental variables of CAMS_Europe")
    exit(-1)

def joinpath(rootdir, targetdir):
    return os.path.join(os.sep, rootdir + os.sep, targetdir)

def valid_datetime(dt):
    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(dt, fmt)
        except ValueError:
            pass
    raise argparse.ArgumentTypeError("Invalid date: '{0}'.".format(dt))

def valid_date(d):
    t = 'T00:00'
    return valid_datetime(d + t)

list_numeric_models = [ "chimere", "ensemble", "EMEP", "LOTOS-EUROS", "MATCH", \
                        "MINNI", "MOCAGE", "SILAM", "EURAD-IM", "DEHM", "GEM-AQ"]

parser = argparse.ArgumentParser(description='Script for downloading CAMS Europe data set')
parser.add_argument('-cams_eu', '--cams_eu', help='chimere - ensemble - EMEP - LOTOS-EUROS - MATCH - MINNI - MOCAGE - SILAM - EURAD-IM - DEHM - GEM-AQ', \
                     choices=list_numeric_models, required=True)
parser.add_argument('-model_level', '--model_level', type=int, required=True)
parser.add_argument('-s_date', '--start_date', metavar='YYYY-MM-DD HH:MM:SS', type=valid_datetime, required=True)
parser.add_argument('-e_date', '--end_date', metavar='YYYY-MM-DD HH:MM:SS', type=valid_datetime, required=True)
args = vars(parser.parse_args())

# Temporal interval to consider
start_datetime = args["start_date"]
end_datetime = args["end_date"]

cams_eu = args["cams_eu"]
model_level = int(args["model_level"])

DATADIR = joinpath(path_main_dir_CAMS_Europe_data, "model_level_" + str(model_level))
DATADIR = joinpath(DATADIR, cams_eu)

# NO2, SO2, CO, O3, PM2.5, PM10: ug m^-3
list_air_pollutants = ["NO2", "O3", "CO", "SO2", "PM2p5", "PM10"]

for air_chem in list_air_pollutants:

    DATADIR_AIR_POLL = joinpath(DATADIR, air_chem)

    if not os.path.exists(DATADIR_AIR_POLL):
        os.mkdir(DATADIR_AIR_POLL)

esito = False
current_date = start_datetime.date()

while esito == False:
    
    string_current_date = str(current_date.year) + "-" + str(current_date.month).zfill(2)
    path_dir_month = joinpath(DATADIR, string_current_date)

    list_files_netcdf = os.listdir(path_dir_month)

    for air_chem in list_air_pollutants:
        
        DATADIR_AIR_POLL = joinpath(DATADIR, air_chem)
        DATADIR_AIR_POLL_MONTH = joinpath(DATADIR_AIR_POLL, string_current_date)

        if not os.path.exists(DATADIR_AIR_POLL_MONTH):
            os.mkdir(DATADIR_AIR_POLL_MONTH)

        for path_file_netcdf in list_files_netcdf:
        
            if air_chem.lower() in path_file_netcdf:

                abs_path_netcdf = joinpath(path_dir_month, path_file_netcdf)
                ds_chem = xr.open_dataset(abs_path_netcdf)

                new_path_netcdf = joinpath(DATADIR_AIR_POLL_MONTH, string_current_date + ".nc")

                # Conversione from ug m^-3 to mg m^-3 --> / 1000
                if air_chem == "CO":
                    ds_chem["co"] /= 1000 

                ds_chem.to_netcdf(new_path_netcdf)

                print(air_chem + " " + string_current_date + " analysed")
                break

    if current_date > end_datetime.date():
        current_date = datetime(end_datetime.year, end_datetime.month, 1, 0, 0).date()
        end_current_date = end_datetime.date()
        esito = True
    else:
        if current_date.month + 1 == 13:
            current_date = datetime(current_date.year + 1, 1, 1, 0, 0).date()
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1, 0, 0).date()