# conda activate CAMS_Europe

# Libreries
import xarray as xr
import os
import shutil
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

list_numeric_models = [ "chimere", "ensemble", "EMEP", "LOTOS-EUROS", "MATCH", \
                        "MINNI", "MOCAGE", "SILAM", "EURAD-IM", "DEHM", "GEM-AQ"]

parser = argparse.ArgumentParser(description='Script for downloading CAMS Europe data set')
parser.add_argument('-cams_eu', '--cams_eu', help='chimere - ensemble - EMEP - LOTOS-EUROS - MATCH - MINNI - MOCAGE - SILAM - EURAD-IM - DEHM - GEM-AQ', \
                     choices=list_numeric_models, required=True)
parser.add_argument('-model_level', '--model_level', type=int, required=True)
args = vars(parser.parse_args())

cams_eu = args["cams_eu"]
model_level = int(args["model_level"])

DATADIR = joinpath(path_main_dir_CAMS_Europe_data, "model_level_" + str(model_level))
path_dir_data = joinpath(DATADIR, cams_eu)

path_save_dir_netcdf = joinpath(path_dir_data, "italy_ext")

if not os.path.exists(path_save_dir_netcdf):
    os.mkdir(path_save_dir_netcdf)

path_save_dir_netcdf = joinpath(path_save_dir_netcdf, cams_eu)

if not os.path.exists(path_save_dir_netcdf):
    os.mkdir(path_save_dir_netcdf)

# Italy coordinates
lat_italy_bnds, lon_italy_bnds = [32,50], [5,21]

for current_dir in os.listdir(path_dir_data):

    abs_path_current_dir = joinpath(path_dir_data,current_dir)

    if os.path.isdir(abs_path_current_dir):
        
        current_path_save_dir_netcdf = joinpath(path_save_dir_netcdf, current_dir)

        if not os.path.exists(current_path_save_dir_netcdf):
            os.mkdir(current_path_save_dir_netcdf)

        print(current_dir + " analysed:")

        for file_netcdf in os.listdir(abs_path_current_dir):
            
            if file_netcdf.endswith('.nc'):
                
                # Opening NetCDF file
                abs_path_file_netcdf = joinpath(abs_path_current_dir, file_netcdf)

                current_ds_chem = xr.open_dataset(abs_path_file_netcdf)

                # Cut of Italy coordinates
                current_italy_ds_chem = current_ds_chem.sel(lat=slice(*lat_italy_bnds), lon=slice(*lon_italy_bnds))

                file_netcdf = "italy_" + file_netcdf

                # Save NetCDF file
                path_file_italy_netcdf = joinpath(current_path_save_dir_netcdf, file_netcdf)
                current_italy_ds_chem.to_netcdf(path_file_italy_netcdf)

                print(file_netcdf + " saved")
