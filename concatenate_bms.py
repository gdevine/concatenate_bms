""" Download, unzip and repackage BMS data from the S39 glasshouse
into a more research reusable format

- Requires Python 2
- Requires 'HIEV_API_KEY' set as environment variable
- Requires 'gmail_user' set as environment variable
- Requires 'gmail_pwd' set as environment variable

Author: Gerry Devine, January 2018

"""

import os
import glob
import sys
import json
import urllib2
import httplib
import zipfile
import re
import pandas as pd
import numpy as np
import requests
import datetime
import logging
import traceback
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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


# Initialize the log file settings and begin logging text block
logging.basicConfig(filename='logfile.log', level=logging.INFO)
logging.info('')
logging.info('----------------')
logging.info('     New Run    ')
logging.info('----------------')
logging.info('Run date: %s' % datetime.date.today())


def send_mail(message, mail_type):
    """ Send email to administrator outlining success or failure of program. """
    sender = "hiedatamanager@gmail.com"
    receiver = "g.devine@westernsydney.edu.au"

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "BMS Conversion - %s!" % mail_type
    msg['From'] = sender
    msg['To'] = receiver

    # Create the body of the message (a plain-text and an HTML version).
    text = "%s" % message
    html = """\
    <html>
      <head></head>
      <body>
        <p><br/><br/>
           %s
        </p>
        <br/>
        <p>See https://github.com/gdevine/concatenate_bms</p> 
      </body>
    </html>
    """ % message

    gmail_user = os.environ['gmail_user']
    gmail_pwd = os.environ['gmail_pwd']

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    # Send the message via local SMTP server.
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    mail = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    mail.ehlo()
    # mail.starttls()
    mail.login(gmail_user, gmail_pwd)
    mail.sendmail(sender, receiver, msg.as_string())
    mail.quit()


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def extract_date(bms_filename):
    """ Extract date in "DEC2017"-type format from given bms zip filename and return it"""
    filename_split = bms_filename.split('.')[0].split('_')
    # return "".join(filename_split[2]+filename_split[3]+filename_split[4])
    month_integer = int(filename_split[3])
    month = datetime.date(1900, month_integer, 1).strftime('%b').upper()
    return "".join(month+filename_split[2])


def file_exists(filename):
    """ Check if a given file exists in HIEv """
    request_headers = {'Content-Type': 'application/json; charset=UTF-8', 'X-Accept': 'application/json'}
    request_data = json.dumps({'auth_token': api_token, 'filename': filename})

    # --Handle the returned response from the HIEv server
    # requests.packages.urllib3.disable_warnings()   # ignore ssl warnings from python 2.7.5
    request = urllib2.Request(request_url, request_data, request_headers)
    response = urllib2.urlopen(request)
    js = json.load(response)
    return len(js) > 1


# Loop over each date in range
# upload_from_date = str(datetime.date.today() - datetime.timedelta(days=1))
# upload_to_date = str(datetime.date.today() - datetime.timedelta(days=0))
upload_from_date = datetime.date(2017, 11, 1)
upload_to_date = datetime.date(2017, 11, 3)


for single_date in daterange(upload_from_date, upload_to_date):
    # First clear out old files in the 'raw' directory
    files = glob.glob(os.path.join(os.path.dirname(__file__), 'raw_data', '*'))
    for f in files:
        os.remove(f)
    # Generate the file name matcher based on the date
    filename = 'BMS_S39_'+str(single_date.year)+'_'+single_date.strftime('%m')+'_'+single_date.strftime('%d')+'.zip'
    # Set up the http request
    request_headers = {'Content-Type': 'application/json; charset=UTF-8', 'X-Accept': 'application/json'}
    # request_data = json.dumps({'auth_token': api_token, 'upload_from_date': upload_from_date,
    #                            'upload_to_date': upload_to_date, 'filename': filename})
    request_data = json.dumps({'auth_token': api_token, 'filename': filename})

    # Send off the request to the HIEv server
    request = urllib2.Request(request_url, request_data, request_headers)

    # Handle the returned response from the HIEv server
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError, e:
        logging.exception('HTTPError = ' + str(e.code))
        send_mail('URLError = ' + str(e.code), 'Error')
        sys.exit()
    except urllib2.URLError, e:
        logging.exception('URLError = ' + str(e.reason))
        send_mail('URLError = ' + str(e.reason), 'Error')
        sys.exit()
    except httplib.HTTPException, e:
        logging.exception('HTTPException')
        send_mail('HTTPException', 'Error')
        sys.exit()
    except Exception as e:
        logging.exception('generic exception: ' + traceback.format_exc())
        send_mail(str(e), 'Error')
        sys.exit()

    # Load in the file, first checking that only one file has been returned
    js = json.load(response)
    if len(js) != 1:
        logging.exception('Search result returned %s results - It should only return one. Date = %s' % (len(js), single_date))
        send_mail('Search result returned %s results - It should only return one. Date = %s ' % (len(js), single_date), 'Error')
        sys.exit()

    zip_file_details = js[0]

    # Grab the month/year from the zip filename
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
    if len(file_list) < 28:
        logging.exception('Unzipped folder contains %s files - There should be at least 28 files. Date = %s' % (len(file_list), single_date))
        send_mail('Unzipped folder contains %s files - There should be at least 28 files. Date = %s' % (len(file_list), single_date), 'Error')
        # sys.exit()
    else:
        logging.info('At least 28 files extracted from zipfile')

    # Loop over each room number and pull in each matching file
    for x in range(1, 9):

        # Check if this month already has data in HIEV for this room (ie this is data from the 2nd of the month onwards)
        if file_exists('S39_R%s_ENVVARS_%s.csv' % (str(x), data_date)):
            logging.info('File found in HIEv for this data stream (room %s) - date will be appended' % str(x))
            #TODO Download current file and append to it
        else:
            logging.info('No file found in HIEv for this data stream (room %s) - date will be created new' % str(x))
            # Create TOA5-based header
            toa5df_1 = pd.DataFrame([["TOA5", "GHS39", "CR3000", "6550", "CR3000.Std.22", "CPU:R3_T1_Flux_20160803.CR3", "50271", "GHS39_R"+str(x)]])
            toa5df_2 = pd.DataFrame([['DateTime', 'Room', 'Temperature', 'Humidity', 'CO2']])
            toa5df_3 = pd.DataFrame([["TS","Number","Degrees C","%%RH","ppm"]])
            toa5df_4 = pd.DataFrame([["", "", "AVE", "AVE", "AVE"]])

            toa5df = pd.concat([toa5df_1, toa5df_2, toa5df_3, toa5df_4])
            # We don't want nans in the header so replace with empty space
            # toa5df.fillna('',inplace=True)


        r = re.compile("hawk-s39_ac_room_%s" % str(x))
        matches = filter(r.match, file_list)

        if len(matches) != 3:
            logging.exception('Unzipped folder contains %s CSV files for room %s - There should be 3. Date = %s' % (len(js), str(x), single_date))
            send_mail('Unzipped folder contains %s CSV files for room %s - There should be 3. Date = %s' % (len(js), str(x), single_date), 'Error')
            # sys.exit()

        for datafile in matches:
            if 'zone_temp' in datafile:
                temp_data = pd.read_csv(os.path.join(raw_dir, datafile), header=0, parse_dates=[0], index_col=0)
                resampled_temp_data = temp_data.resample('5T', closed='right', label='right').mean()
            elif 'zone_humidity' in datafile:
                humidity_data = pd.read_csv(os.path.join(raw_dir, datafile))['Value']
                # Use this file for grabbing 5-minutely datetime series
                date_times = pd.read_csv(os.path.join(raw_dir, datafile))['DateTime']
                # convert the date time to a datetime format
                date_converter = lambda y: datetime.datetime.strptime(y, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
                date_times = date_times.apply(date_converter)
            elif 'co2_sensor' in datafile:
                co2_data = pd.read_csv(os.path.join(raw_dir, datafile), header=0, parse_dates=True, index_col=0)
                resampled_co2_data = co2_data.resample('5T', closed='right', label='right').mean()
            else:
                logging.warning('New data file name found for room %s!' % str(x))
                send_mail('New data file name found for room %s!' % str(x), 'Error')

        # Bring together all variable series' into one dataframe (and add room number column)
        rooms = pd.DataFrame([str(x)]*288)
        # in the event of missing sensor data replace with NaNs
        try:
            resampled_co2_data
        except:
            resampled_co2_data = pd.DataFrame([str(np.nan)]*288)
        try:
            resampled_temp_data
        except:
            resampled_temp_data = pd.DataFrame([str(np.nan)]*288)
        try:
            humidity_data
        except:
            humidity_data = pd.Series([str(np.nan)]*288)

        date_times = pd.Series(pd.date_range(str(single_date.year)+"-"+single_date.strftime('%m')+"-"+single_date.strftime('%d')+" 00:05:00", periods=288, freq="5min"))

        combined_data = pd.concat([date_times, rooms, resampled_temp_data.reset_index(drop=True), humidity_data, resampled_co2_data.reset_index(drop=True)], axis=1)

        final_toa5 = pd.concat([toa5df, combined_data])

        # Output to new CSV file
        combined_data.to_csv(os.path.join(output_dir, 'S39_R%s_ENVVARS_%s.csv' % (str(x), data_date)), index=False,
                             header=['DateTime', 'Room', 'Temperature', 'Humidity', 'CO2'])
        logging.info('CSV file created: %s' % 'S39_R%s_ENVVARS_%s.csv' % (str(x), data_date))


        # -------------------------------------------------------------------------
        # Set metadata variables for upload
        experiment_id     = 43
        upload_url       = 'https://hiev.uws.edu.au/data_files/api_create.json?auth_token='+api_token
        filetype          = 'RAW'
        description       = "This dataset contains environment monitoring data for room %s in the S39 glasshouse facility based at the " \
                            "Hawkesbury Insitute for the Environment at the University of Western Sydney. Each dataset contains " \
                            "daily CSV data covering the variables CO2 Sensor (ppm), Zone Humidity (%%RH) and Zone Temperature (degrees Celsius). Both CO2 and temperature measurements " \
                            "have been averaged to 5 minutes (from originally minutely data) with the timestamp equating to the point at which values have been averaged forward to." % str(x)
        creator_email     = 'g.devine@westernsydney.edu.au'
        contributor_names = ['a.gherlanda@westernsydney.edu.au']
        label_names       = '"Glasshouse","Temperature","CO2","Humidity"'
        start_time        = str(date_times.iloc[0])
        end_time          = str(date_times.iloc[-1])
        # -------------------------------------------------------------------------

        # load the file for uploading via the HIEv API
        files = {'file': open(os.path.join(output_dir, 'S39_R%s_ENVVARS_%s.csv' % (str(x), data_date)), 'rb')}

        # Compile available metadata
        payload = {'type':          filetype,
                   'experiment_id': experiment_id,
                   'start_time':    start_time,
                   'end_time':      end_time,
                   'description':   description,
                   'label_names':   label_names,
                   'creator_email':   creator_email,
                   'contributor_names[]':   contributor_names,
                   }

        # # Upload file and associated metadata to HIEv
        # requests.packages.urllib3.disable_warnings()   # ignore ssl warnings from python 2.7.5
        # r = requests.post(upload_url, files=files, data=payload, verify=False)
        #
        # # Print the outcome of the upload
        # if r.status_code == 200:
        #     print 'File successfully uploaded to HIEv'
        # else:
        #     print 'ERROR - There was a problem uploading the file to HIEv'

# Finally clear out the last batch of files from the raw directory and those files from the 'output' directory (as
# they are now safely in HIEv)
rawfiles = glob.glob(os.path.join(os.path.dirname(__file__), 'raw_data', '*'))
outputfiles = glob.glob(os.path.join(os.path.dirname(__file__), 'output_data', '*'))
# for r_f in rawfiles:
    # os.remove(r_f)
# for o_f in outputfiles:
#     os.remove(o_f)

# Run successful - send notification email
send_mail('BMS files successfully reorganised and concatenated', 'Success')
