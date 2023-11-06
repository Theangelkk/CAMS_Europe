# conda activate CAMS_Europe

# Libreries
import numpy as np
import xarray as xr
from zipfile import ZipFile
import os
import argparse
from datetime import datetime, timedelta
import warnings

# API Request
import cdsapi

warnings.filterwarnings("ignore")

def joinpath(rootdir, targetdir):
    return os.path.join(os.sep, rootdir + os.sep, targetdir)

# Numeric models for model level 55: about 288mt

# [0] chimere from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [1] ensemble from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [2] EMEP from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [3] LOTOS-EUROS from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [4] MATCH         (TO DO)
# [5] MINNI         (TO DO)
# [6] MOCAGE from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [7] SILAM from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [8] EURAD-IM from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [9] DEHM from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)
# [10] GEM-AQ from 2019 to 2020 (validated_reanalysis) -- from 2021 to 2022 (interim_reanalysis)

# Numeric models for surface level: about 10mt

# [0] chimere from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)
# [1] ensemble  from 2013 to 2015 [NO2 - O3 - PM2,5 - PM10] (validated_reanalysis) 
#               from 2016 to 2020 (validated_reanalysis) 
#               from 2021 to 2021 (interim_reanalysis)
# [2] EMEP from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)
# [3] LOTOS-EUROS from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)
# [4] MATCH from 2020 (validated_reanalysis) -- from 2021 (interim_reanalysis)
# [5] MINNI from 2020 (validated_reanalysis)
# [6] MOCAGE from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)
# [7] SILAM from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)
# [8] EURAD-IM from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)
# [9] DEHM from 2018 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)
# [10] GEM-AQ from 2019 to 2020 (validated_reanalysis) -- from 2021 to 2021 (interim_reanalysis)

# Path of CAMS Europe
path_main_dir_CAMS_Europe_data = os.environ['CAMS_Europe']

if path_main_dir_CAMS_Europe_data == "":
    print("Error: set the environmental variables of CAMS_Europe")
    exit(-1)

if not os.path.exists(path_main_dir_CAMS_Europe_data):
  os.mkdir(path_main_dir_CAMS_Europe_data)

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
parser.add_argument('-start_year', '--start_year', type=int, required=True)
parser.add_argument('-end_year', '--end_dend_yearate', type=int, required=True)
args = vars(parser.parse_args())

cams_eu = args["cams_eu"]

# Temporal interval to consider
start_year = int(args["start_year"])
end_year = int(args["end_year"])

model_level = int(args["model_level"])

if start_year > end_year:
    print("Error: end_year must be greater than start_year")
    exit(-1)

DATADIR = joinpath(path_main_dir_CAMS_Europe_data, "model_level_" + str(model_level))

if not os.path.exists(DATADIR):
  os.mkdir(DATADIR)

DATADIR = joinpath(DATADIR, cams_eu)

if not os.path.exists(DATADIR):
  os.mkdir(DATADIR)

if model_level == 60:
    model_level = 0
elif model_level == 55:
    model_level = 250

# Italy coordinates
lat_italy_bnds, lon_italy_bnds = [32,50], [5,21]

c = cdsapi.Client(url='https://ads.atmosphere.copernicus.eu/api/v2', key='15833:3671670b-6ec7-47c5-9483-8175ee8648c9')

list_years = [*range(start_year, end_year)]

for idx_year in range(len(list_years)):

    for i in range(1,13):

        first_month = str(i).zfill(2)

        name_file_zip = str(list_years[idx_year]) + "-" + first_month + ".zip"
        PATH_ZIP_FILE = joinpath(DATADIR, name_file_zip)

        if list_years[idx_year] <= 2020:
            string_type = 'validated_reanalysis'
        else:
            string_type = 'interim_reanalysis'

        if not os.path.exists(PATH_ZIP_FILE):
            
            # Richiesta dei dati a CAMS
            c.retrieve(
                'cams-europe-air-quality-reanalyses',
                {
                    'type': string_type,
                    'year': str(list_years[idx_year]),
                    'format': 'zip',
                    'variable': [
                        'carbon_monoxide', 'nitrogen_dioxide', 'ozone',
                        'particulate_matter_10um', 'particulate_matter_2.5um', 'sulphur_dioxide',
                    ],
                    'model': cams_eu.lower(),
                    'level': str(model_level),
                    'month': [
                        first_month,
                    ],
                },
                PATH_ZIP_FILE)