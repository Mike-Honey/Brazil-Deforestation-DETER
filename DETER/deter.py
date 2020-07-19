import geopandas
import numpy
import os
import pandas_profiling

from urllib import request, parse
import zipfile
import shapefile
import datetime
from json import dumps, loads
from mapbox import Uploader
from dotenv import load_dotenv
from time import sleep
from random import randint
load_dotenv()

import os
import subprocess

"""
Make sure you have a .env file in the directory where this script is containing the following:

  MAPBOX_TOKEN=sk.somelongstringswithadot.anotherstring-mend
  MAPBOX_USER=yourusername

"""

mapbox_user = os.getenv("MAPBOX_USER")
mapbox_access_token = os.getenv("MAPBOX_TOKEN")

def uploadToMapbox(datadir):
  """
  Upload all the geojson files from a given directory to mapbox as tilesets
  """
  for file in os.listdir(datadir):
    filename = os.fsdecode(file)
    if filename.endswith(".geojson"): 
      print(os.path.splitext(file)[0])
      mapboxUpload(os.path.join(datadir, file), os.path.splitext(file)[0])
      continue
    else:
      continue

def mapboxUpload(filename):
  """
  See: https://mapbox-mapbox.readthedocs-hosted.com/en/latest/uploads.html#uploads
  """

  #raise NotImplementedError("MapboxUpload is not implemented yet")

  service = Uploader()
  service.session.params['access_token'] = mapbox_access_token
  mapid = 'uploads-test' # 'uploads-test'
  with open(filename, 'rb') as src:
    upload_resp = service.upload(src, mapid)

    if upload_resp.status_code == 422:
      for i in range(5):
        sleep(5)
        with open(filename, 'rb') as src:
          upload_resp = service.upload(src, mapid)
        if upload_resp.status_code != 422:
          break
    
    upload_id = upload_resp.json()['id']
    for i in range(5):
      status_resp = service.status(upload_id).json()
      if status_resp['complete']:
        print(status_resp)
        print("Finished uploading tileset " + mapid)
        break
      sleep(5)

def writegeojson(filename, key, dict):
  filename_out = filename + "-" + key + ".geojson"
  geojson = open(filename_out, "w")
  geojson.write(dumps({"type": "FeatureCollection",\
    "features": dict[key]}, indent=2, default=myconverter) + "\n")
  geojson.close()
  print("writegeojson - Wrote file: " + filename_out)

def writecsv(filename, key, dict):
  filename_out = filename + "-" + key + ".csv"
  csv = open(filename + "-" + key + ".csv", "w")
  print (";".join(dict[key][0]['properties'].keys()), file=csv)
  for item in dict[key]:
    itemvaluelist = list(item['properties'].values())
    itemvaluelist[0] = str(itemvaluelist[0])
    itemvaluelist[4] = itemvaluelist[4].strftime('%Y-%m-%d')
    itemvaluelist[8] = ('%.16f' % itemvaluelist[8]).rstrip('0').rstrip('.')
    itemvaluelist[9] = ('%.16f' % itemvaluelist[9]).rstrip('0').rstrip('.')
    itemvaluelist[10] = ('%.16f' % itemvaluelist[10]).rstrip('0').rstrip('.')
    print (";".join(itemvaluelist), file=csv)
  csv.close()
  print("writecsv - Wrote file: " + filename_out)

def myconverter(o):
  """
  DateTime objects need to be converted otherwise json will throw an error 
  See: https://code-maven.com/serialize-datetime-object-as-json-in-python
  """

  if isinstance(o, datetime.date):
        return o.strftime('%Y-%m-%d')


def shape2geojson(filename):
  """
  When the zip is downloaded and converted to shape, 
  we pass the filename to shape2geojson
  to create a geojson file.
  We can do a lot of extra's here, like split the geojson to multiple files.
  """

  # read the shapefile
  reader = shapefile.Reader(filename)
  fields = reader.fields[1:]
  field_names = [field[0] for field in fields]
  field_names.insert(0,"ROW_NUMBER")
  months = []
  
  # We gather all the unique months..
  for sr in reader.shapeRecords():
    try:
      i = months.index(sr.record[3].strftime('%Y%m'))
    except ValueError:
      months.append(sr.record[3].strftime('%Y%m'))

  # Then turn it into a dictionary with empty arrays..
  monthsdict = dict()
  months.sort() 
  for m in months:
    monthsdict.update({m: []})

  # Add the features to the dictionary for the given months..
  for sr in reader.shapeRecords():
    rawatt = sr.record
    rawatt.insert(0,sr.record.oid)
    atr = dict(zip(field_names, rawatt))
    m = sr.record[4].strftime('%Y%m')
    geom = sr.shape.__geo_interface__
    monthsdict[m].append(dict(type="Feature", \
        geometry=geom, properties=atr))

  # write a GeoJSON file for each month
  for key in monthsdict:
    writegeojson(filename, key, monthsdict)
    writecsv(filename, key, monthsdict)
    # End of processing.

# Added by Mike
def shapeaddrownumber(filename, outfilename):
  """
  When the zip is downloaded and converted to shape, 
  we pass the filename to shapeaddrownumber to add the ROW_NUMBER field.
  """

  # read the shapefile
  reader = shapefile.Reader(filename)
  fields = reader.fields[1:]
  field_names = [field[0] for field in fields]
  field_names.insert(0,"ROW_NUMBER")
  monthsdict = dict()
  months = ['2020']
  for m in months:
    monthsdict.update({m: []})

  # Add the features to the dictionary for the given months..
  for sr in reader.shapeRecords():
    rawatt = sr.record
    rawatt.insert(0,sr.record.oid)
    atr = dict(zip(field_names, rawatt))
    m = sr.record[4].strftime('%Y%m')
    geom = sr.shape.__geo_interface__
    if m >= '202001':
      monthsdict[months[0]].append(dict(type="Feature", \
          geometry=geom, properties=atr))

  # write a GeoJSON file 
  writegeojson(outfilename, months[0], monthsdict)
    # End of processing.


def getunzipped(theurl, thedir):
  """
  Download a file from a url and unzip it
  """

  name = os.path.join(thedir, 'deter-amz_all.zip')
  try:
    name, hdrs = request.urlretrieve(theurl, name)
    print ("getunzipped - Downloaded:" + name)
  except IOError as e:
    print ("Can't retrieve %r to %r: %s" % (theurl, thedir, e))
    return

  try:
    z = zipfile.ZipFile(name)
  except zipfile.error as e:
    print ("Bad zipfile (from %r): %s" % (theurl, e))
    return
  z.extractall(thedir)
  z.close()
  print("getunzipped - Unzipped: " + name + " to: " + thedir)
  #os.unlink(name)


def shape_process(filename, outfilename, VIEW_DATE_prefix):
  """
  When the zip is downloaded and extracted, filter for year and process.
  """
  input_gdf = geopandas.read_file(filename, encoding="utf-8")
  print (input_gdf)
  # profile = pandas_profiling.ProfileReport(input_gdf.drop('geometry',axis=1))
  # profile.to_file(filename + ".html")
  input_gdf_year = input_gdf[input_gdf['VIEW_DATE'].str.startswith(VIEW_DATE_prefix)]
  input_gdf_year['ROW_NUMBER'] = numpy.arange(len(input_gdf_year))
  print (input_gdf_year)
  output_gdf_year_wgs84 = input_gdf_year.to_crs('epsg:4326')
  filename_out = outfilename + "-" + VIEW_DATE_prefix + ".geojson"
  output_gdf_year_wgs84.to_file(filename_out, driver='GeoJSON', encoding="utf-8")
  print("shape_process - Wrote file: " + filename_out)


def main():
  """
  Main - program execute
  """

  #datadir = '../data/'
  datadir = './'
  getunzipped('http://terrabrasilis.dpi.inpe.br/file-delivery/download/deter-amz/shape', datadir) 
 
  # shapeaddrownumber(datadir + 'deter_public.shp', datadir + 'deter_all.shp' )
  # shape_process(datadir + 'deter_public.shp', datadir + 'deter_all.shp', '2019' )
  shape_process(datadir + 'deter_public.shp', datadir + 'deter_all.shp', '2020' )
  
  # Command line to run:
  # "C:/cygwin64/home/mikehoney/tippecanoe/tippecanoe" -o "C:/dev/INPE/DETER/deter-amz.mbtiles" -f -Z 0 -z 9 "C:/dev/INPE/DETER/deter_all.shp-2020.geojson"
  # subprocess.call ('C://cygwin64//home//mikehoney//tippecanoe//tippecanoe -o C://dev//INPE//DETER//deter-amz-2019.mbtiles -f -Z 0 -z 9 C://dev//INPE//DETER//deter_all.shp-2019.geojson' , shell=True)
  subprocess.call ('C://cygwin64//home//mikehoney//tippecanoe//tippecanoe -o C://dev//INPE//DETER//deter-amz.mbtiles -f -Z 0 -z 9 C://dev//INPE//DETER//deter_all.shp-2020.geojson' , shell=True)

  #uploadToMapbox(datadir)
  exit()

if __name__ == '__main__':
  main()

