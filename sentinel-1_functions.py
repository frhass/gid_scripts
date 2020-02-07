import snappy
from snappy import ProductIO
import os, gc
from snappy import GPF
from snappy import jpy


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
