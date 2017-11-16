""" Download, unzip and repackage BMS data from the S39 glasshouse
into a more research reusable format

- Requires Python 2
- Requires 'HIEV_API_KEY' set as environment variable

"""


import os
import sys
import json
import urllib2
import httplib
import zipfile
import re
import pandas as pd
import numpy as np
import datetime
import logging
import traceback


def send_mail(message):
    """ Send email to administrator outlining success or failure of program. """
    print 'sending email with message: %s' % message


def extract_date(bms_filename):
    """ Extract date in YYYYMMDD format from given bms zip filename"""
    filename_split = bms_filename.split('.')[0].split('_')
    return "".join(filename_split[2]+filename_split[3]+filename_split[4])


# Initialize the log file settings and begin logging text block
logging.basicConfig(filename='logfile.log', level=logging.INFO)
logging.info('')
logging.info('')
logging.info('----------------')
logging.info('     New Run    ')
logging.info('----------------')
logging.info('Run date: %s' % datetime.date.today())


# Set up global values
api_token = os.environ['HIEV_API_KEY']
request_url = 'https://hiev.westernsydney.edu.au/data_files/api_search'


# Create 'raw' and 'output' folders to hold data (if not already existing)
raw_dir = os.path.join(os.path.join(os.path.dirname(__file__), 'raw_data'))
output_dir = os.path.join(os.path.join(os.path.dirname(__file__), 'output_data'))
if not os.path.exists(raw_dir):
    os.makedirs(raw_dir)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# Provide search filter parameters
filename = 'BMS_S39_'
upload_from_date = str(datetime.date.today() - datetime.timedelta(days=1))
upload_to_date = str(datetime.date.today() - datetime.timedelta(days=0))


# Set up the http request
request_headers = {'Content-Type': 'application/json; charset=UTF-8', 'X-Accept': 'application/json'}
request_data = json.dumps({'auth_token': api_token, 'upload_from_date': upload_from_date,
                           'upload_to_date': upload_to_date, 'filename': filename})


# Handle the returned response from the HIEv server
request = urllib2.Request(request_url, request_data, request_headers)

try:
    response = urllib2.urlopen(request)
except urllib2.HTTPError, e:
    logging.exception('HTTPError = ' + str(e.code))
    send_mail('URLError = ' + str(e.code))
    sys.exit()
except urllib2.URLError, e:
    logging.exception('URLError = ' + str(e.reason))
    send_mail('URLError = ' + str(e.reason))
    sys.exit()
except httplib.HTTPException, e:
    logging.exception('HTTPException')
    send_mail('HTTPException')
    sys.exit()
except Exception as e:
    logging.exception('generic exception: ' + traceback.format_exc())
    send_mail(str(e))
    sys.exit()


# Load in the file, first checking that only one file has been returned
js = json.load(response)
if len(js) != 1:
    logging.exception('Search result returned %s results - It should only return one ' % len(js))
    send_mail('Search result returned %s results - It should only return one ' % len(js))
    sys.exit()

zip_file_details = js[0]


# Grab the date from the zip filename
data_date = extract_date(str(zip_file_details['filename']))


# Pass the url to the download API and download
download_url = zip_file_details['url']+'?'+'auth_token=%s' % api_token
request = urllib2.Request(download_url)
f = urllib2.urlopen(request)
with open(os.path.join(raw_dir, zip_file_details['filename']), 'w') as local_file:
    local_file.write(f.read())
    logging.info('Zip file downloaded: %s' % str(zip_file_details['filename']))

local_file.close()


# Unzip the file into the separate CSV files
zip_ref = zipfile.ZipFile(os.path.join(raw_dir, zip_file_details['filename']), 'r')
zip_ref.extractall(raw_dir)
zip_ref.close()


# Check that the correct number of files are contained within the unzipped directory
file_list = os.listdir(raw_dir)
if len(file_list) != 28:
    logging.exception('Unzipped folder contains %s CSV files - There should be 27 ' % len(file_list))
    send_mail('Unzipped folder contains %s CSV files - There should be 27 ' % len(file_list))
    sys.exit()
else:
    logging.info('28 files extracted from zipfile')


# Loop over each room number and pull in each matching file
for x in range(1, 9):
    r = re.compile("hawk-s39_ac_room_%s" % str(x))
    matches = filter(r.match, file_list)

    if len(matches) != 3:
        logging.exception('Unzipped folder contains %s CSV files for room %s - There should be 3 ' % (len(js), str(x)))
        send_mail('Unzipped folder contains %s CSV files - There should be 3 ' % len(js))
        sys.exit()

    for datafile in matches:
        if 'zone_temp' in datafile:
            temp_data = pd.read_csv(os.path.join(raw_dir, datafile))['Value']
            temp_data = temp_data.groupby(np.arange(len(temp_data)) // 5).mean()
        elif 'zone_humidity' in datafile:
            date_times = pd.read_csv(os.path.join(raw_dir, datafile))['DateTime']
            humidity_data = pd.read_csv(os.path.join(raw_dir, datafile))['Value']
        elif 'co2_sensor' in datafile:
            co2_data = pd.read_csv(os.path.join(raw_dir, datafile))['Value']
            co2_data = co2_data.groupby(np.arange(len(co2_data)) // 5).mean()
        else:
            logging.warning('New data file name found!')

    # Bring together all data into one dataframe and write iot to file
    combined_data = pd.concat([date_times, temp_data, humidity_data, co2_data], axis=1)

    # Decide where to append to existing data (if not new month) or create new file
    combined_data.to_csv(os.path.join(output_dir, 'S39_R%s_ENVVARS_%s.csv' % (str(x), data_date)), index=False)
    logging.info('CSV file created: %s' % 'S39_R%s_ENVVARS_%s.csv' % (str(x), data_date))


print 'done'
