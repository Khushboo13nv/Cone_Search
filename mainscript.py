import os
import sys
import math
import pyvo
from pyvo.dal import tap
import requests
import cgi
import re
import json
import getpass
import numpy as np
from astropy.time import Time
from astropy.coordinates import SkyCoord
import ast
from astroquery.simbad import Simbad
from dateutil import parser
from typing import Tuple, Union, Optional, List


# Authorization function:
def getToken(username, password):
    """Token based authentication to ESO: provide username and password to receive back a JSON Web Token."""
    if username==None or password==None:
        return None
    token_url = "https://www.eso.org/sso/oidc/token"
    token = None
    try:
        response = requests.get(token_url,
                            params={"response_type": "id_token token", "grant_type": "password",
                                    "client_id": "clientid",
                                    "username": username, "password": password})
        token_response = json.loads(response.content)
        token = token_response['id_token']
    except NameError as e:
        print(e)
    except:
        print(f"AUTHENTICATION ERROR: Invalid credentials provided for username {username}") 

    return token

# Function for downloading files:
def download_asset(file_url, filename=None, token=None):
    """
    Download and rename raw or calibrated files. It return  status of process and filename.

    Parameters: 
        file_url (str): URL of the file to be downloaded
    
    Returns:
        tuple: (response.status_code, filename)
    """

    headers = None
    if token!=None:
        headers = {"Authorization": "Bearer " + token}
        response = requests.get(file_url, headers=headers)
    else:
        # Trying to download anonymously:
        response = requests.get(file_url, stream=True, headers=headers)

    if filename == None:
        contentdisposition = response.headers.get('Content-Disposition')
        if contentdisposition != None:
            value, params = cgi.parse_header(contentdisposition)
            filename = params["filename"]
        
        if filename == None:
            filename = 'downloaded_file.fits'

    if os.path.exists(filename):
        skip = 'yes'???

        return (200, filename, skip)

    if response.status_code == 200:
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=50000):
                f.write(chunk)

    return (response.status_code, filename, skip)

def calselector_info(description):
    """Parse the main calSelector description, and fetch: category, complete, certified, mode, and messages."""

    category=""
    complete=""
    certified=""
    mode=""
    messages=""
    
    m = re.search('category="([^"]+)"', description)
    if m:
        category=m.group(1)
    m = re.search('complete="([^"]+)"', description)
    if m:
        complete=m.group(1).lower()
    m = re.search('certified="([^"]+)"', description)
    if m:
        certified=m.group(1).lower()
    m = re.search('mode="([^"]+)"', description)
    if m:
        mode=m.group(1).lower()
    m = re.search('messages="([^"]+)"', description)
    if m:
        messages=m.group(1)

    return category, complete, certified, mode, messages

def print_calselector_info(description, mode_requested):
    """Print the most relevant params contained in the main calselector description."""

    category, complete, certified, mode_executed, messages = calselector_info(description)

    alert=""
    if complete!= "true":
        alert = "ALERT: incomplete calibration cascade"

    mode_warning=""
    if mode_executed != mode_requested:
        mode_warning = "WARNING: requested mode (%s) could not be executed" % (mode_requested)

    certified_warning=""
    if certified != "true":
        certified_warning = "WARNING: certified=\"%s\"" %(certified)

    print("    calibration info:")
    print("    ------------------------------------")
    print("    science category=%s" % (category))
    print("    cascade complete=%s" % (complete))
    print("    cascade messages=%s" % (messages))
    print("    cascade certified=%s" % (certified))
    print("    cascade executed mode=%s" % (mode_executed))
    print("    full description: %s" % (this_description))
   
    return alert, mode_warning, certified_warning


# Functions for Input parameters:
def get_target_and_instruments() -> Tuple[str, Tuple[str, ...]]:
    target_name = input("Enter target name:").strip()
    instruments_input = input("Enter instruments (comma-separated, e.g., ESPRESSO,HARPS,NIRPS,FEROS): ").strip()
    instruments = tuple([inst.strip() for inst in instruments_input.split(',') if inst.strip()])
    
    return target_name, instruments
    
def get_dates() -> Tuple[str, str]:
    
    start_date_input = input("Start date of observational data (DD-MM-YYYY): ").strip()
    end_date_input = input("End date of observational data (DD-MM-YYYY): ").strip()
    
    # Parse to datetime object:
    start_date = parser.parse(start_date_input, dayfirst=True)
    end_date = parser.parse(end_date_input, dayfirst=True)
    
    # Convert to ISO format (to seconds precision):
    start_date_iso = start_date.isoformat(timespec='seconds')
    end_date_iso   = end_date.isoformat(timespec='seconds')

    return start_date_iso, end_date_iso

def get_radius() -> float:
    radius_input = input('Radius for cone search in arcmin: ').strip()
    radius = (float(radius_input))/60.0  # radius in degree
    
    return radius

def input_parameters() -> Tuple[str, Tuple[str, ...], Union[str, None], Union[str, None], float]:
    target_name, instruments = get_target_and_instruments()
    radius = get_radius()
    
    date           = input('Do you want to apply filters on date? [y/n]: ').strip().lower()
    frame_type_raw = input('Do you want to download raw data? [y/n]: ').strip().lower()

    if date.lower() == 'y':
        start_date_iso, end_date_iso = get_dates()
    else:
        start_date_iso, end_date_iso = None, None

    return target_name, instruments, radius, start_date_iso, end_date_iso, date, frame_type_raw


# Target name and Coordinate resolver:
def resolve_target(target_name: str):
    """
    Resolves a target name using SIMBAD and returns its main id, ra, and dec.

    Parameters:
    -----------
        target_name (str): The name of the astronomical target.

    Returns:
    --------
        tuple: (target_name, ra, dec) rounded to six decimal places.
    
    Raises:
    -------
        ValueError: If the object is not found in SIMBAD.
    """
    search = Simbad.query_object(target_name)

    if search is None:
        raise ValueError(f"Target '{target_name}' not found in SIMBAD.")

    target_main_id      = search['main_id'][0]
    ra                  = round(float(search['ra'][0]), 6)
    dec                 = round(float(search['dec'][0]), 6)

    return target_main_id, ra, dec
      

class QueryBuilder:
    def __init__(self, table: str):
        self.table = table
        self.conditions: List[str] = []
        self.select_fields = "*"

    def add_condition(self, condition: str):
        if condition:
            self.conditions.append(condition)

    def build_query(self) -> str:
        """
        Constructs the full SQL query string with clean formatting.

        Returns:
            str: A properly formatted SQL query.
        """
        if not self.conditions:
            raise ValueError("No conditions specified for the query.")

        query_lines = [
            f"SELECT {self.select_fields}",
            f"FROM {self.table}",
            f"WHERE {self.conditions[0]}"
        ]

        for condition in self.conditions[1:]:
            query_lines.append(f"AND {condition}")

        return "\n".join(query_lines)

def query_selector(param, raw_data: str = 'y', date_filter: str = 'y') -> str:
    """
    Build a SQL query with parameters properly handled.
    """
    contained = " '', ra, dec " if raw_data.lower() == 'y' else " '', s_ra, s_dec "
    table = "dbo.raw" if raw_data.lower() == 'y' else "ivoa.ObsCore"
    qb = QueryBuilder(table=table)

    spatial_condition = f"CONTAINS(POINT({contained}), CIRCLE('', {param['ra']}, {param['dec']}, {param['radius']})) = 1"
    instrument_field = 'instrument' if raw_data.lower() == 'y' else 'instrument_name'
    instrument_list = ",".join(f"'{inst}'" for inst in param['instruments']) 
    instrument_condition = f"{instrument_field} IN ({instrument_list})"

    if raw_data.lower() == 'y':
        qb.add_condition("NOT target = 'SUN'")
        qb.add_condition(spatial_condition)
        qb.add_condition(instrument_condition)

        if date_filter.lower() == 'y':
            qb.add_condition(f"exp_start BETWEEN '{param['start_date']}' AND '{param['end_date']}' ")
        
        qb.add_condition("dp_cat = 'SCIENCE'")

    else:
        qb.add_condition(spatial_condition)
        qb.add_condition(instrument_condition)
        qb.add_condition("dataproduct_type = 'spectrum'")

        if date_filter.lower() == 'y':
            qb.add_condition(f"t_min BETWEEN {param['start_date_mjd']} AND {param['end_date_mjd']}")

    return qb.build_query()


#=============================================================
# MAIN
#=============================================================

mode_requested = "raw2raw"
#mode_requested = "raw2master"

thisscriptname = os.path.basename(__file__)  
headers = {}
headers={'User-Agent': '%s (ESO script drc %s)'%(requests.utils.default_headers()['User-Agent'], thisscriptname)}


ESO_TAP_OBS = "http://archive.eso.org/tap_obs"
tapobs = tap.TAPService(ESO_TAP_OBS)

# Authentication:
# ---------------
print("Authenticating...")
username = input("Type your ESO username: ")
password = getpass.getpass(prompt=f"{username} user's password: ", stream=None)

token = getToken(username, password)
if token == None:
    print("Could not authenticate. Continuing as anonymous!")
else:
    print("Authentication successful!")

print()


# Querying:
# --------
print(f"Querying the ESO TAP service at {ESO_TAP_OBS}")
target_name, instruments, radius, start_date_iso, end_date_iso, date, frame_type_raw = input_parameters()

try:
    target_main_id, ra, dec = resolve_target(target_name)
except ValueError as e:
    print(e)

parameters = {
    "ra": ra,
    "dec": dec,
    "radius": radius,
    "instruments": instruments,
    "start_date": start_date_iso,
    "end_date": end_date_iso,
    "start_date_mjd": Time(start_date_iso, scale='utc').mjd if start_date_iso is not None else None,
    "end_date_mjd": Time(end_date_iso, scale='utc').mjd if end_date_iso is not None else None}

query = query_selector(parameters, raw_data=frame_type_raw, date_filter=date)
#print(query)

frames = tapobs.search(query=query)
table = frames.to_table()

print(table)

nfiles = 0
calib_association_trees = []

print("Downloading %d files and their calibration cascades (trying with %s mode)." % (len(frames.to_table()), mode_requested))
print("Note: Even if present in the cascade, siblings are not downloaded by this script.")
print("")


for row in frames:
    if nfiles >= 5:
        break 
    
    # RETRIEVE THE SCIENCE RAW FRAME OR SPECTRUM:
    #--------------------------------------------
    sci_url = "https://dataportal.eso.org/dataportal_new/file/%s" % row["dp_id"]
    status, sci_filename, skipped = download_asset(sci_url, token=token)
    print("SCIENCE: %4d/%d dp_id: %s downloaded"  % (nfiles, len(frames), sci_filename))


    if frame_type_raw == 'y':  
        calselector_url = "http://archive.eso.org/calselector/v1/associations?dp_id=%s&mode=%s&responseformat=votable" % (row["dp_id"], mode_requested)

        datalink = pyvo.dal.adhoc.DatalinkResults.from_result_url(calselector_url)
    
        # PRINT CASCADE INFORMATION AND MAIN DESCRIPTION
        # ----------------------------------------------
        this_description=next(datalink.bysemantics('#this')).description 
        alert, mode_warning, certified_warning = print_calselector_info(this_description, mode_requested)

        # create and use a mask to get only the #calibration entries:
        calibrators = datalink['semantics'] == '#calibration'
        calib_urls = datalink.to_table()[calibrators]['access_url','eso_category']

        # DOWNLOAD EACH #calibration FILE FROM THE LIST
        #----------------------------------------------
        for i_calib, (url, category) in enumerate(calib_urls):
            if i_calib >= 3:
                break

            status, filename, skipped = download_asset(url)
            if not skipped:
                if status==200:
                    print("    CALIB: %4d/%d dp_id: %s (%s) downloaded"  % (i_calib, len(calib_urls), filename, category))
                else:
                    print("    CALIB: %4d/%d dp_id: %s (%s) NOT DOWNLOADED (http status:%d)"  % (i_calib, len(calib_urls), filename, category, status))
            else:
                print(f"File {filename} already exists. Skipping downloading.")
        
        # PRINT ANY ALERT OR WARNING ENCOUNTERED BY THE PROCESS THAT GENERATES THE CALIBRATION CASCADE:
        #---------------------------------------------------------------------------------------------
        if alert!="":
            print("    %s" % (alert))
        if mode_warning!="":
            print("    %s" % (mode_warning))
        if certified_warning!="":
            print("    %s" % (certified_warning))
    
    nfiles += 1 

    print("------------------------------------------------------------------------------------------------")
     
     