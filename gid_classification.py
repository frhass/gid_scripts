import snappy
from snappy import ProductIO
from snappy import HashMap
import os, gc
from snappy import GPF
from snappy import jpy, ProgressMonitor
from datetime import datetime
from osgeo import gdal, ogr
import shutil

### Loading ProgressMonitor
PrintPM = jpy.get_type('com.bc.ceres.core.PrintWriterProgressMonitor')
ConcisePM = jpy.get_type('com.bc.ceres.core.PrintWriterConciseProgressMonitor')
System = jpy.get_type('java.lang.System')
pm = PrintPM(System.out)

### Working folders
preprocessed = r"S:\users\...\godthaab_iceberg_detection\images\sentinel_1\preprocessed"
processed = r"S:\users\...\godthaab_iceberg_detection\images\sentinel_1\processed"
outputdir = "S:\\users\\...\\godthaab_iceberg_detection\\images\\sentinel_1\\classification\\"
mask_path = r"S:\users\...\godthaab_iceberg_detection\mask\OceanMask.shp"
p_folder = r"\\...\Geodata\godthaab_iceberg_detection"
pros_folder = r"S:\users\...\godthaab_iceberg_detection\images\sentinel_1\processing"

GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
HashMap = snappy.jpy.get_type('java.util.HashMap')
gc.enable()

### Remove content of p_folder
for f in os.listdir(p_folder):
    os.remove(os.path.join(p_folder, f))

### Looping through files in preprocessed folder 
for dirs in os.listdir(preprocessed):
    subdir = os.path.join(preprocessed, dirs)
    for file in os.listdir(subdir):
        file_split = file.split("_")[6]
        ### Specify which polarization will be classified
        if file_split == 'HH.tif':
            print("Processing...", file_split)
            print("processingfile:", file)
            sen1_file = os.path.join(subdir, file)
            sentinel_1 = ProductIO.readProduct(sen1_file)

            ### Naming
            filename_split = subdir.split("\\")[7]
            filename = str(filename_split)
            input_undersplit = filename.split("_")[:5]
            out_dir = os.path.join(outputdir, filename)
            datestamp = str(input_undersplit[4])
            date = datestamp[:8]
            time = datestamp[9:]

            if not os.path.exists(out_dir):
               os.mkdir(out_dir)

            ### AdaptiveThresholding (Constant False Alarm Rate CFAR)
            parameters = HashMap()
            parameters.put('targetWindowSizeInMeter', 20)
            parameters.put('guardWindowSizeInMeter', 200.0)
            parameters.put('backgroundWindowSizeInMeter', 800.0)
            parameters.put('pfa', 10.0)
            print("cfar Parameters:", parameters)

            target_1 = GPF.createProduct("AdaptiveThresholding", parameters, sentinel_1)
            parameters = None

            ### Export subset (sigma_0_HH)
            parameters = HashMap()
            parameters.put('bandNames', 'Sigma0_HH')

            target_2 = GPF.createProduct("Subset", parameters, target_1)
            sigma_out = pros_folder + "\\" + filename
            ProductIO.writeProduct(target_2, sigma_out, 'GeoTIFF-BigTIFF', pm)
            target_2.dispose()
            parameters = None

            ### Subset (extracting classification band)
            parameters = HashMap()
            parameters.put('bandNames', 'Sigma0_HH_ship_bit_msk')

            class_out = pros_folder + "\\" + filename + "_cfar"
            print("class_out:", class_out)
            target_3 = GPF.createProduct("Subset", parameters, target_1)
            ProductIO.writeProduct(target_3, class_out, 'GeoTIFF-BigTIFF', pm)
            print("CFAR written to tiff file, at:", datetime.now())
            target_3.dispose()


            ### Classification to vector
            temp_tiff = class_out + ".tif"
            print("temp_tiff:", temp_tiff)
            temp_out = out_dir
            gdal.Translate(temp_out, temp_tiff, format='GTIFF', noData='0',) # (output tiff, tiff to handle,,,)
            gdal.Translate(out_dir + "\\" + filename + ".tif", sigma_out + ".tif", format='GTIFF', noData='0', )

            ds = gdal.Open(str(temp_tiff))

            rasterband = ds.GetRasterBand(1)

            dst_layername = out_dir + "\\" + filename + "_Iceberg_outline"
            drv = ogr.GetDriverByName("GeoJSON")
            dst_ds = drv.CreateDataSource(dst_layername + ".geojson")
            dest_srs = ogr.osr.SpatialReference()
            dest_srs.ImportFromEPSG(3857)
            dst_layer = dst_ds.CreateLayer(dst_layername, dest_srs)

            gdal.Polygonize(rasterband, rasterband, dst_layer, -1, [], callback=None)
            # gdal.Polygonize(input, mask, dest_layer, -1, [], callback=None)

	    ### Make copy of output to network folder
            shutil.make_archive(p_folder + "\\" + filename, 'zip', out_dir) #(output zip folder, zip, folder to zip)
    
    ### Moving folder from pre-processed to processed folder
    shutil.move(subdir, processed)
