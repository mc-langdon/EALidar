from qgis.processing import alg
from qgis.core import QgsCoordinateReferenceSystem
from qgis.core import QgsProcessing
import processing
import json
import requests
import time
import pandas as pd
import os.path
import urllib.request
import zipfile
import glob

# Defines a function that checks the processing status of the DEFRA download website after sending the polygon.
def getStatus(ID):
	url = f"https://environment.data.gov.uk/arcgis/rest/services/gp/DataDownload/GPServer/DataDownload/jobs/{ID}"

	querystring = {"f":"json","dojo.preventCache":""}

	payload = ""
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
		"Accept": "*/*",
		"Accept-Language": "en-GB,en;q=0.5",
		"Accept-Encoding": "gzip, deflate, br",
		"DNT": "1",
		"Connection": "keep-alive",
		"Referer": "https://environment.data.gov.uk/DefraDataDownload/?Mode=survey",
		"Cookie": "AGS_ROLES=419jqfa+uOZgYod4xPOQ8Q==",
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors",
		"Sec-Fetch-Site": "same-origin"
	}

	statusGet = requests.request("GET", url, data=payload, headers=headers, params=querystring) #this sends the url GET request
	statusStr = statusGet.text # converts the response to a string
	status = json.loads(statusStr) # loads the response into a json object
	return status["jobStatus"] # returns jobStatus



@alg(name="lidar_downloader", label=alg.tr("Lidar Downloader"), group="matt", group_label=alg.tr("Matt's Scripts"))
# 'INPUT' is the recommended name for the main input parameter
@alg.input(type=alg.SOURCE, name="INPUT", label="Input layer")
# 'WORK_DIR' is where the downloaded files will be stored.
@alg.input(type=alg.FOLDER_DEST, name="WORK_DIR", label="Working Directory")
# 'OUTPUT' is the recommended name for the main output parameter
@alg.input(type=alg.FILE_DEST, name="OUTPUT", label="Merged output layer", fileFilter='VRT files (*.vrt *.VRT)')
# 'CLIP' is a boolean parameter that determines whether to clip or not clip the result
@alg.input(type=alg.BOOL, name="CLIP", label="Clip result")
# 'CLIP_OUTPUT' is the temporary merge ouput if a clip is being performed.
@alg.input(type=alg.RASTER_LAYER_DEST, name='CLIP_OUTPUT', label='Clipped output layer', optional=True)
# For more decorators check https://docs.qgis.org/latest/en/docs/user_manual/processing/scripts.html#the-alg-decorator
def testalg(instance, parameters, context, feedback, inputs):
	"""
	This Script accepts a single polygon as an input and uses the hidden API of the DEFRA Survey Data Download website to access relevant Lidar tiles.
	
	Currently this is limited to accessing the Composite DTM with a 2m resolution.
	
	Source: https://environment.data.gov.uk/DefraDataDownload/?Mode=survey
	"""
	# Nested function that allows the script to wait until the processing on the DEFRA website has completed.
	def waitUntil(jobID, timeout, period = 5):
		mustEnd = time.time() + timeout
		while time.time() < mustEnd:
			if getStatus(jobID)=="esriJobSucceeded": return True
			feedback.pushInfo(getStatus(jobID))
			time.sleep(period)
			if feedback.isCanceled():
				return {}
			
		return False
	
	feedback.setProgressText('Searching for lidar tiles... (Step 1/4)')
	feedback.setProgress(0)
	
	outputs = {}
	workDir = parameters['WORK_DIR']
	# Convert multipart polygons to single part as DEFRA website appears to expect this.
	alg_params = {
		'INPUT': parameters['INPUT'],
		'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
	}
	spPolyIn = processing.run('native:multiparttosingleparts', alg_params, context=context,feedback=feedback, is_child_algorithm=True)
	spPolyStr = spPolyIn['OUTPUT']
	spPolyOut = context.takeResultLayer(spPolyStr)
	# Need to work out how to select only the first feature.
	feats = spPolyOut.getFeatures()
	for feat in feats: # Converts feature to json format for upload to Defra website.
		geom = feat.geometry()
		a = geom.asJson()
		b = json.loads(a)
	coords = b["coordinates"]
	feedback.pushInfo("JSON Co-ordinates: " + str(coords))
	
	if feedback.isCanceled():
		return {}

	url = "https://environment.data.gov.uk/arcgis/rest/services/gp/DataDownload/GPServer/DataDownload/submitJob"

	querystring = {"f":"json","SourceToken":"","OutputFormat":"0","RequestMode":"SURVEY","AOI":"{\"geometryType\":\"esriGeometryPolygon\",\"features\":[{\"geometry\":{\"rings\":"+str(coords)+",\"spatialReference\":{\"wkid\":27700,\"latestWkid\":27700}}}],\"sr\":{\"wkid\":27700,\"latestWkid\":27700}}"}
	payload = ""
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
		"Accept": "*/*",
		"Accept-Language": "en-GB,en;q=0.5",
		"Accept-Encoding": "gzip, deflate, br",
		"DNT": "1",
		"Connection": "keep-alive",
		"Referer": "https://environment.data.gov.uk/DefraDataDownload/?Mode=survey",
		"Cookie": "AGS_ROLES=419jqfa+uOZgYod4xPOQ8Q==",
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors",
		"Sec-Fetch-Site": "same-origin",
		"If-None-Match": "1293cb6f",
		"Content-Type": "multipart/form-data; boundary=---011000010111000001101001"
	}

	response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

	job = json.loads(response.text)
	jobID = job["jobId"]
	feedback.pushInfo("DEFRA JobID: "+jobID)
	
	waitUntil(jobID,120)
	
	url = f"https://environment.data.gov.uk/arcgis/rest/directories/arcgisjobs/gp/datadownload_gpserver/{jobID}/scratch/results.json"

	feedback.setProgressText('Dowloading lidar tiles... (Step 2/4)')
	feedback.setProgress(25)

	payload = ""
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
		"Accept": "*/*",
		"Accept-Language": "en-GB,en;q=0.5",
		"Accept-Encoding": "gzip, deflate, br",
		"DNT": "1",
		"Connection": "keep-alive",
		"Referer": "https://environment.data.gov.uk/DefraDataDownload/?Mode=survey",
		"Cookie": "AGS_ROLES=419jqfa+uOZgYod4xPOQ8Q==",
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors",
		"Sec-Fetch-Site": "same-origin"
	}

	response = requests.request("GET", url, data=payload, headers=headers)

	tilesStr = response.text
	tiles = json.loads(tilesStr)
	
	flattened = pd.json_normalize(tiles, record_path = ["data","years","resolutions","tiles"],meta = [["data","productName"],["data","years","resolutions","resolutionName"]], errors = "ignore") #refer to https://towardsdatascience.com/all-pandas-json-normalize-you-should-know-for-flattening-json-13eae1dfb7dd

	urls = flattened[flattened["data.productName"].isin(["LIDAR Composite DTM","LIDAR Composite Last Return DSM","National LIDAR Programme DSM","National LIDAR Programme DTM"])]
	links = urls['url'] [(urls["data.productName"] == "LIDAR Composite DTM") & (urls["data.years.resolutions.resolutionName"] == "DTM 2M")]

	#download the files
	files = [] #create blank list of files

	for link in links:
		
		if feedback.isCanceled():
			return {}
		
		link = link.strip()
		name = link.rsplit("/",1)[-1]
		filename = os.path.join(workDir, name)
		feedback.pushInfo("downloading: " + filename)
		try:
			urllib.request.urlretrieve(link, filename)
			files.append(filename)
		except Exception as inst:
			feedback.reportError(inst)
			feedback.reportError("   Encountered unknown error. Continuing")

	feedback.setProgressText('Unzipping lidar tiles... (Step 3/4)')
	feedback.setProgress(50)

	unzippedFiles = [] #create blank list of files

	for file in files:
		
		if feedback.isCanceled():
			return {}
		
		fileName = os.path.basename(file)
		name = os.path.splitext(fileName)[0]
		targetDir = os.path.join(workDir, name)
		with zipfile.ZipFile(file,"r") as zipRef:
			feedback.pushInfo("Extracting " + file + " to " + targetDir)
			zipRef.extractall(targetDir)
		unzippedFiles.append(targetDir)

	globPattern = workDir + r"\**\*.tif"
	DL_Tiles = glob.glob(globPattern, recursive=True)

	if feedback.isCanceled():
		return {}

	feedback.setProgressText('Merging lidar tiles... (Step 4/4)')
	feedback.setProgress(75)

	#OUTPUT
	alg_params = {
		'INPUT':DL_Tiles,
		'RESOLUTION':0,
		'SEPARATE':False,
		'PROJ_DIFFERENCE':False,
		'ADD_ALPHA':False,
		'ASSIGN_CRS':None,
		'RESAMPLING':0,
		'SRC_NODATA':-3.40282e+38,
		'EXTRA':'',
		'OUTPUT': parameters['OUTPUT']
	}
	outputs['merged'] = processing.runAndLoadResults('gdal:buildvirtualraster', 
		alg_params, context=context, feedback=feedback)
	
	if feedback.isCanceled():
		return {}

	clipStatus = parameters['CLIP']
	
	if clipStatus:
		alg_params = {
			'INPUT':outputs['merged']['OUTPUT'],
			'MASK':parameters['INPUT'],
			'SOURCE_CRS':None,
			'TARGET_CRS':None,
			'TARGET_EXTENT':None,
			'NODATA':-3.40282e+38,
			'ALPHA_BAND':False,
			'CROP_TO_CUTLINE':True,
			'KEEP_RESOLUTION':False,
			'SET_RESOLUTION':False,
			'X_RESOLUTION':None,
			'Y_RESOLUTION':None,
			'MULTITHREADING':False,
			'OPTIONS':'',
			'DATA_TYPE':0,
			'EXTRA':'',
			'OUTPUT':parameters['CLIP_OUTPUT']
		}
		outputs['clipped'] = processing.run("gdal:cliprasterbymasklayer",
			alg_params, context=context, feedback=feedback, is_child_algorithm=True)
	return {"OUTPUT": outputs}
	
