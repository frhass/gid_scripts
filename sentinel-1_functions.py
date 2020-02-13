import snappy
from snappy import ProductIO
import os, gc
from snappy import GPF
from snappy import jpy
import ogr
import gdal


### Loading progress monitor
PrintPM = jpy.get_type('com.bc.ceres.core.PrintWriterProgressMonitor')
System = jpy.get_type('java.lang.System')
pm = PrintPM(System.out)


rootpath = "S:\\path\\to\\root\\folder"
dem = r"S:\path\to\dem\demfile_WGS84.tif"
mask = r"S:\path\to\oceanmask\OceanMask.shp"

def prepros(scene_folder):

    ### Folder, date & timestamp
    preprocessed = rootpath + "\\preprocessed"
    scene_name = str(scene_folder.split("\\")[X])[:32] # [X] element of filepath, change this to match scene name. [:32] first caracters of element
    print("scene_name:", scene_name)
    output_folder = os.path.join(preprocessed, scene_name)
    print("output_folder:", output_folder)

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
    HashMap = snappy.jpy.get_type('java.util.HashMap')
    gc.enable()

    sentinel_1 = ProductIO.readProduct(scene_folder + "\\manifest.safe")

    pols = ['HH', 'HV'] # Be aware of polarizations for different scenes
    for p in pols:
        polarization = p

        ### Apply-Orbit-File
        parameters = HashMap()
        parameters.put('orbitType', 'Sentinel Precise (Auto Download)')
        parameters.put('polyDegree', 3)
        parameters.put('continueOnFail', True)

        target_0 = GPF.createProduct("Apply-Orbit-File", parameters, sentinel_1)
        del parameters

        ### Calibration
        parameters = HashMap()
        parameters.put('outputSigmaBand', True)
        parameters.put('sourceBands', 'Intensity_' + polarization)
        parameters.put('selectedPolarisations', polarization)
        parameters.put('outputImageScaleInDb', False)

        target_1 = GPF.createProduct("Calibration", parameters, target_0)
        del target_0
        del parameters

        ### Terrain-Correction
        parameters = HashMap()
        parameters.put('demResamplingMethod', 'NEAREST_NEIGHBOUR')
        parameters.put('imgResamplingMethod', 'NEAREST_NEIGHBOUR')
        parameters.put('demName', 'External DEM')
        parameters.put('externalDEMFile', dem)
        parameters.put('externalDEMNoDataValue', -9999.0) # DEM nodata value
        parameters.put('pixelSpacingInMeter', 20.0) # DEM pixelsize
        parameters.put('sourceBands', 'Sigma0_' + polarization)
        parameters.put('saveSelectedSourceBand', True)
        parameters.put('nodataValueAtSea', False)

        target_2 = GPF.createProduct("Terrain-Correction", parameters, target_1)
        del target_1
        del parameters

        ### Import-Vector
        parameters = HashMap()
        parameters.put('vectorFile', mask)
        parameters.put('separateShapes', False)

        target_3 = GPF.createProduct('Import-Vector', parameters, target_2)
        del target_2
        del parameters

        ### Land-Sea-Mask
        parameters = HashMap()
        parameters.put('useSRTM', False)
        parameters.put('landMask', True)
        parameters.put('geometry', 'OceanMask_250mBuffer_rep') # input shapefile name

        out_file = output_folder + "\\" + scene_name + "_preprocessed_" + polarization
        target_4 = GPF.createProduct("Land-Sea-Mask", parameters, target_3)
        ProductIO.writeProduct(target_4, out_file, 'GeoTIFF-BigTIFF', pm)
        del target_3
        del parameters
        
    
def classify(prepros_folder):
    for file in os.listdir(prepros_folder):

        ### Folder, date & timestamp
        classification = rootpath + "\\classification"
        scene_name = str(prepros_folder.split("\\")[X])[:32]  # [X] element of filepath, change this to match scene name. [:32] first caracters of element
        print("scene_name:", scene_name)
        output_folder = os.path.join(classification, scene_name)
        print("output_folder:", output_folder)
        polarization = str(file)[46:48] # polarization from filename

        if not os.path.exists(output_folder):
            os.mkdir(output_folder)

        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
        HashMap = snappy.jpy.get_type('java.util.HashMap')
        gc.enable()

        sentinel_1 = ProductIO.readProduct(os.path.join(prepros_folder, file))

        res = [20, 50]  # parameters for CFAR
        for r in res:
            resolution = r

            gd_window = resolution * 12.0
            bg_window = resolution * 37.0

            # AdaptiveThresholding (Constant False Alarm Rate CFAR)
            parameters = HashMap()
            parameters.put('targetWindowSizeInMeter', resolution)
            parameters.put('guardWindowSizeInMeter', gd_window)
            parameters.put('backgroundWindowSizeInMeter', bg_window)
            parameters.put('pfa', 6.0)

            target_1 = GPF.createProduct("AdaptiveThresholding", parameters, sentinel_1)
            parameters = None

            # Subset (extracting classification band)
            parameters = HashMap()
            parameters.put('bandNames', 'Sigma0_' + polarization + '_ship_bit_msk')

            outfile = output_folder + "\\" + scene_name + "_" + polarization + "_cfar_" + str(resolution) + "m"
            target_2 = GPF.createProduct("Subset", parameters, target_1)
            ProductIO.writeProduct(target_2, outfile, 'GeoTIFF-BigTIFF', pm)

            # Classification to vector
            ds = gdal.Open(str(outfile + ".tif"))

            rasterband = ds.GetRasterBand(1)

            dst_layername = outfile + "_Iceberg_outline"
            drv = ogr.GetDriverByName("GeoJSON")
            dst_ds = drv.CreateDataSource(dst_layername + ".geojson")
            dest_srs = ogr.osr.SpatialReference()
            dest_srs.ImportFromEPSG(4326)
            dst_layer = dst_ds.CreateLayer(dst_layername, dest_srs)

            gdal.Polygonize(rasterband, rasterband, dst_layer, -1, [], callback=None) # (input, mask, dest_layer,,,) 
            
######################################################################################################################

def merge(classification_folder):
    scene_name = str(classification_folder.split("/")[6])[:32]
    output_file = os.path.join(classification_folder, scene_name + "_merge.shp")

    df = gpd.GeoDataFrame()

    fileList = os.listdir(classification_folder)

    # Merging geojson files
    for file in fileList:
        if file.endswith('.geojson'):
            print("file:", file)
            file = classification_folder + "/" + file

            jsondf = gpd.GeoDataFrame.from_file(file)

            for idx, row in jsondf.iterrows():
                df = df.append(row, ignore_index=True)

    # Union all rows into single multipolygon
    print("Merging features...")
    union_df = gpd.GeoSeries(unary_union(df['geometry']))

    # Explode multipolygon into seperate features
    print("Exploding polygons")
    indf = gpd.GeoDataFrame(geometry=gpd.GeoSeries(union_df))
    explode_df = gpd.GeoDataFrame(columns=indf.columns)

    for idx, row in indf.iterrows():
        if type(row.geometry) == Polygon:
            explode_df = explode_df.append(row, ignore_index=True)
        if type(row.geometry) == MultiPolygon:
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row] * recs, ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom, 'geometry'] = row.geometry[geom]
            explode_df = explode_df.append(multdf, ignore_index=True)

    print(explode_df.info)

    explode_df.crs = {'init': 'epsg:4326'}
    explode_df.to_file(output_file)

    print("Done")
