import shutil
import snappy
from snappy import ProductIO
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import os, gc
from snappy import GPF
from snappy import jpy
from snappy import ProgressMonitor
from datetime import datetime

### Loading ProgressMonitor 
PrintPM = jpy.get_type('com.bc.ceres.core.PrintWriterProgressMonitor')
ConcisePM = jpy.get_type('com.bc.ceres.core.PrintWriterConciseProgressMonitor')
System = jpy.get_type('java.lang.System')
pm = PrintPM(System.out)

### Basic parameters and working folders
path = "S:\\users\\aeh\\godthaab_iceberg_detection\\images\\sentinel_1"
processed_path = path + "\\preprocessed\\"
processing_dir = os.path.join(path, "processing")
dem = r"S:\users\...\godthaab_iceberg_detection\arcticdem\demfile.tif"
mask_path = r"S:\users\...\godthaab_iceberg_detection\mask\OceanMask.shp"
pros_folder = r"S:\users\...\godthaab_iceberg_detection\images\sentinel_1\processing"

### Clearing the processing folder
for f in os.listdir(pros_folder):
    os.remove(os.path.join(pros_folder, f))

### Looping over files in download folder
zip_archives = []
for file in os.listdir(os.path.join(path, "downloaded")):
    if file.endswith(".SAFE"):

        shutil.move(os.path.join(path, "downloaded", file), processing_dir)

        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
        HashMap = snappy.jpy.get_type('java.util.HashMap')

        gc.enable()
	
	### Setting up filenames
        folder = os.path.join(processing_dir, file)
        output = folder
        timestamp = file.split("_")[:4]
        print("Timestamp:", timestamp)
        date = file.split("_")[4:5]
        List_to_str_date = ''.join(date)
        date = List_to_str_date[0:15]
        print("date:", date)

        sentinel_1 = ProductIO.readProduct(output + "\\manifest.safe")

	### Creating endfolder for pre-processed product
        pp_endfolder = processed_path + '_'.join(timestamp) + '_' + date
        if not os.path.exists(pp_endfolder):
            os.mkdir(pp_endfolder)
        print("pp_endfolder:", pp_endfolder)

	### Looping over polarizations of each scene
        pols = ['HH', 'HV']
        for p in pols:
            polarization = p

            print("#########################################")
            print("#########################################")
            print("Preprocessing:", file, ", Polarization:", p, "....", datetime.now())

            ### Apply-Orbit-File
            parameters = HashMap()
            parameters.put('orbitType', 'Sentinel Precise (Auto Download)')
            parameters.put('polyDegree', 3)
            parameters.put('continueOnFail', True)

            target_0 = GPF.createProduct("Apply-Orbit-File", parameters, sentinel_1)
            parameters = None

            ### Calibration
            parameters = HashMap()
            parameters.put('outputSigmaBand', True)
            parameters.put('sourceBands', 'Intensity_' + polarization)
            parameters.put('selectedPolarisations', polarization)
            parameters.put('outputImageScaleInDb', False)

            target_1 = GPF.createProduct("Calibration", parameters, target_0)
            del target_0
            parameters = None

            ### Terrain-Correction
            parameters = HashMap()
            parameters.put('demResamplingMethod', 'NEAREST_NEIGHBOUR')
            parameters.put('imgResamplingMethod', 'NEAREST_NEIGHBOUR')
            parameters.put('demName', 'External DEM')
            parameters.put('externalDEMFile', dem)
            parameters.put('externalDEMNoDataValue', -9999.0)
            parameters.put('pixelSpacingInMeter', 20.0)
            parameters.put('sourceBands', 'Sigma0_' + polarization)
            parameters.put('saveSelectedSourceBand', True)
            parameters.put('nodataValueAtSea', False)

            target_2 = GPF.createProduct("Terrain-Correction", parameters, target_1)
            del target_1
            parameters = None

            ### Import-Vector
            parameters = HashMap()
            parameters.put('vectorFile', mask_path)
            parameters.put('separateShapes', False)

            target_3 = GPF.createProduct('Import-Vector', parameters, target_2)
            del target_2
            parameters = None

            ### Land-Sea-Mask
            parameters = HashMap()
            parameters.put('useSRTM', False)
            parameters.put('geometry', 'OceanMask_250mBuffer_rep')

            target_4 = GPF.createProduct("Land-Sea-Mask", parameters, target_3)
            del target_3
            parameters = None

            ### Reproject
            parameters = HashMap()
            parameters.put('crs', '3857')

            reproject = pp_endfolder + "\\" + '_'.join(timestamp) + '_' + date + "_preprocessed_" + polarization
            target_5 = GPF.createProduct("Reproject", parameters, target_4)
            ProductIO.writeProduct(target_5, reproject, 'GeoTIFF-BigTIFF', pm)
            del target_4
            parameters = None
