import os
import shutil
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import datetime
import pandas as pd
from zipfile import ZipFile

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

### Basic parameters, working folders, etc.
working_folder = r"S:\users\aeh\godthaab_iceberg_detection"
platformname = "sentinel_1"
imagedir = os.path.join(working_folder, "images", platformname, "downloaded")
zipfile_dir = os.path.join(imagedir, "zip")
scriptpath = os.path.dirname(os.path.realpath(__file__))
today = datetime.now().strftime("%Y%m%d")

### List containing orbitnumber and slice of the relevant scenes
orbitnumber_slice = [[54, 1], [127, 5], [25, 5]]

### Create api connection to sentinelhub using sentinelsat
api = SentinelAPI('username', 'password', 'https://scihub.copernicus.eu/dhus')

### Search by polygon, time, and SciHub query keywords
products_df = pd.DataFrame()
for scene in orbitnumber_slice:
    print(scene)
    products = api.query(date=('NOW-7DAYS', 'NOW'),
                         platformname='Sentinel-1',
                         sensoroperationalmode='IW',
                         producttype='GRD',
                         polarisationmode='HH HV',
                         relativeorbitnumber=scene[0],
                         slicenumber=scene[1])


    ### Convert to Pandas DataFrame
    df = api.to_dataframe(products)
    products_df = products_df.append(df)

#print(products_df.to_string())


### Sort and possibly limit to first 1 sorted products
products_df_sorted = products_df.sort_values(['endposition'], ascending=[False])
# products_df_sorted = products_df_sorted.head(1)
# print("Before list:", products_df_sorted.to_string())

### Changing scene names
platformname = products_df_sorted['platformname'].values[0].replace("-","_").lower()
#platformname = "sentinel_1"


### Check if scenes already exists in zipfile folder
if os.path.exists(zipfile_dir):
    already_downloaded = [f for f in os.listdir(zipfile_dir) if f.endswith(".zip")]
else:
    pass

already_downloaded = [i.split('.zip')[0] for i in already_downloaded]
# print(already_downloaded)

### Filter out already downloaded scenes
for i in already_downloaded:
    indexNames = products_df_sorted[products_df_sorted['identifier'] == str(i)].index
    products_df_sorted.drop(indexNames, inplace=True)

print("Product list:", products_df_sorted.to_string())

### Download sorted and reduced products
if not os.path.exists(imagedir):
    os.makedirs(imagedir)
api.download_all(products_df_sorted['uuid'], imagedir)


##Unzipping
zip_archives = []
for file in os.listdir(imagedir):
    print("Working with " + file)
    if file.endswith(".zip"):
        zip = ZipFile(os.path.join(imagedir, file))
        zip.extractall(imagedir)
        zip.close()
        if not os.path.exists(zipfile_dir):
            os.makedirs(zipfile_dir)
        shutil.move(os.path.join(imagedir, file), zipfile_dir)
