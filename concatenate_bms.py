""" Download, unzip and repackage BMS data from the S39 glasshouse
into a more research reusable format

- Requires Python 2
- Requires 'HIEV_API_KEY' set as environment variable
- Requires 'gmail_user' set as environment variable
- Requires 'gmail_pwd' set as environment variable

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
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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


def extract_date(bms_filename):
    """ Extract date in YYYYMMDD format from given bms zip filename"""
    filename_split = bms_filename.split('.')[0].split('_')
    return "".join(filename_split[2]+filename_split[3]+filename_split[4])


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


# Provide search filter parameters
filename = 'BMS_S39_'
upload_from_date = str(datetime.date.today() - datetime.timedelta(days=1))
upload_to_date = str(datetime.date.today() - datetime.timedelta(days=0))


# Set up the http request
request_headers = {'Content-Type': 'application/json; charset=UTF-8', 'X-Accept': 'application/json'}
request_data = json.dumps({'auth_token': api_token, 'upload_from_date': upload_from_date,
                           'upload_to_date': upload_to_date, 'filename': filename})


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
    logging.exception('Search result returned %s results - It should only return one ' % len(js))
    send_mail('Search result returned %s results - It should only return one ' % len(js), 'Error')
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
    logging.exception('Unzipped folder contains %s files - There should be 28' % len(file_list))
    send_mail('Unzipped folder contains %s files - There should be 28' % len(file_list), 'Error')
    sys.exit()
else:
    logging.info('28 files extracted from zipfile')


# Loop over each room number and pull in each matching file
for x in range(1, 9):
    r = re.compile("hawk-s39_ac_room_%s" % str(x))
    matches = filter(r.match, file_list)

    if len(matches) != 3:
        logging.exception('Unzipped folder contains %s CSV files for room %s - There should be 3 ' % (len(js), str(x)))
        send_mail('Unzipped folder contains %s CSV files - There should be 3 ' % len(js), 'Error')
        sys.exit()

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
            logging.warning('New data file name found!')

    # Bring together all variable series' into one dataframe
    combined_data = pd.concat([date_times, resampled_temp_data.reset_index(drop=True), humidity_data, resampled_co2_data.reset_index(drop=True)], axis=1)


    # Decide whether to append to existing data (if not new month) or create new file
    combined_data.to_csv(os.path.join(output_dir, 'S39_R%s_ENVVARS_%s.csv' % (str(x), data_date)), index=False)
    logging.info('CSV file created: %s' % 'S39_R%s_ENVVARS_%s.csv' % (str(x), data_date))


# Run successful - send notification email
send_mail('BMS files successfully reorganised and concatenated', 'Success')
