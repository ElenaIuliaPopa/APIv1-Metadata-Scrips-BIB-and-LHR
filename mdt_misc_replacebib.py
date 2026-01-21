#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Copyright (c) 2025 by OCLC
#
#  File		    : mdt_misc_addbib.py
#  Description	: Script to add/create records in WC with MAPIv2. It uses TS268 auth.
#  Author(s)	: Elena Popa
#  Creation	    : 19-03-2025
#
#  History:
#  19-03-2025	: popae    : creation
#
#  Notes	    : 
#
#  SVN ident	: $Id$

# Built-in/Generic Imports
import os
import sys
import argparse
import fnmatch
import subprocess
import threading  # to refresh the token
import glob
from requests.exceptions import RetryError  # Import the correct exception

#import yaml
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import datetime
import sys
import json
import re
import time
import xml.etree.ElementTree as ET

#============================================================#
#                   START OF HELP PARSER
#============================================================#
parser = argparse.ArgumentParser(
    description='Script to download LHRs  through the MAPIv2' ,
    formatter_class=argparse.RawTextHelpFormatter  # # This prevents argparse from reformatting help text
)

parser._optionals.title = 'Options' #Customize options title if desired
parser._positionals.title = 'Mandatory arguments' #Customize positionals title if desired

# Positional (mandatory) argument:
parser.add_argument("-i", "--in", dest="input_file", required=True, help=(
                                                'Input file to be processed or "file_pattern".\n'
                                                '- "file_pattern" requires double quotes.\n'
                                                "- Input file is an mrc file with LHRs to be added.\n"
                                                "- No 001 field should be present.\n"
 )
)

parser.add_argument("-m", "--mode", nargs="?", required=True, choices=['a'], help=(
                                                "'[a]dd'.\n"
 )
)

'''
parser.add_argument("-s", "--symbol", nargs="?", required=True, choices=['TS268', 'AAE'], help=(
                                                "- The WSKey that you want to use.\n"
                                                "- 'TS268' or 'AAE'.\n"
 )
)
'''

# Optional arguments
parser.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity; use as argumnet AFTER the input file.')

# Parse the arguments
args = parser.parse_args()

#============================================================#
#                   END OF HELP PARSER
#============================================================#


if args.verbose:
    print("Verbose mode is ON.\n")

# Ensure input_file is provided if -morehelp is not used
if args.input_file is None:
    parser.error("The following arguments are required: input_file. Type -h or --help or -morehelp for more information")

input_arg = args.input_file


# serviceURL = config.get('metadata_service_url')
serviceURL = 'https://metadata.api.oclc.org/worldcat'

# Scope for BIBS
scope = ['WorldCatMetadataAPI:manage_bibs', 'WorldCatMetadataAPI:view_brief_bib']


wskey = OAuth2Session(client=client)

token = None
max_retries = 3
timeout_token = 50
timeout_request = 50   #default timeout for the API
max_retries = 10
retry_delay = 3

is_first_line = True

def get_ocn(record):

    ocn = None
    
    result = subprocess.run(
        ["csfn_marc2norm", "-n", "UNX", "-o", "OCLCHPB"],
        input=record,
        capture_output=True,
        check=True
    )

    nrm_record = result.stdout.decode("utf-8")

    print(f"NRM Record: {nrm_record}")

    fields = nrm_record.splitlines()

    for field in fields:
        if field.startswith('001'):
            ocn = field[6:].strip()
                    
    
    print(f"OCN:{ocn}")
    return ocn
    #sys.exit(0)

def main(file_name):
    
    token = fetch_token()
    if token is None:
        print("Fetching token in failed")
    else:
        if args.verbose:
            print("Token in main fetched")
    
    with open(file_name, 'rb') as file:
        data_in_file = file.read()

    records = data_in_file.split(b'\x1D') # GS split -but it stores the last record as an empty string

    #Check if last record is empty
    if records[-1].strip() == b'': 
        records = records[:-1] #Exclude the last empty record
        records_count = len(records)
        print(f"Nr. of records found: {records_count}")
    else:
        records_count = len(records)
        print(f"Nr. of records found: {records_count}")


    nr = 0
    for record in records:
        nr += 1
        ocn = get_ocn(record)
        process_record(record, ocn, nr)
    
def process_record(record, ocn, nr):    
    #print("I am here 1")
    try:
        def request_data(record, ocn):
            r = wskey.put(serviceURL + f"/manage/bibs/{ocn}", data=record, headers={"Accept": "application/marc", "Content-Type":"application/marc"})
            r.raise_for_status
            result = r.content
            result = result.decode("UTF-8")

            #print(f"{result}\n")
            #input("Press Enter to continue...")

            return result
            
            # How an error result looks like:
            # xml or json
            """
            {
                "type": "NOT_ACCEPTABLE",
                "title": "Invalid 'Accept' header.",
                "detail": "A request with an invalid 'Accept' header was attempted: ...."
            }
            """

            # How a good result looks:
            #mrc binary file
            """
             00260nx  a2200121zi 4500001001000000004000700010005001700017007000300034008003300037852002900070876001100099538002800110\u001E227503625....\u001E\u001D      
            """
        #================ DEF FOR API ===============================>                    
        
        with open(output6, 'a') as out6, open(output1, 'a') as out1, open(output2, 'a') as out2:
            processed = False
            for attempt in range(max_retries): 
                try:
                    # Send the record to the API Request
                    result = request_data(record, ocn)
                    
                    # Process the response from the API
                    if 'API rate limit exceeded' in result:
                        print(f"API rate limit exceeded.Exiting program...\n")
                        sys.exit(0)
                    
                    elif (
                            'API Key or Authorization header is required' in result or 
                            '<!DOCTYPE html>' in result or  
                            '<head><title>502 Bad Gateway</title></head>' in result
                         ):

                        out6.write(f"Attempt {attempt+1} - Error :\n{result}\n")
                        out6.flush()
                        
                        print(f'Error encountered:\n***{result}***')
                        print(f'Retrying request\n')
                        
                        # Refresh token and try again
                        fetch_token()
                        #result = request_data(ctrl_nr) This only sends to function, does not check for error again, it assume succes
                        continue  # Go to next attempt to retry
                            
                            
                    # If no error found, proceed                                
                    
                    #mcrc returned - thus good
                    if '\x1E' in result:
                        out1.write(f"{result}")
                        print(f"BIB replaced Successfully: {nr}\n")

                    # bad requests go in a different file because of xml
                    elif '<type>BAD_REQUEST</type>' in result:
                        print(f"Bad Request\n")
                        out2.write(f"{result}") # Bad request xml response
                                   
                    # Any other errors that are not caputred above in the log.
                    # No retrying as the problem is not related to the request but to the record itself or the request is not valid.
                    else:
                        print(f"Went wrong.\n")
                        out6.write(f"{result}") # 


                    processed = True
                    break  # Exit retry loop and move to next record
                    #return result

                except RetryError:
                    #print(f"-->Retry failed after {max_retries} attempts.")
                    error_msg = f"--> Retry failed after {max_retries} attempts for record nr {nr}.\n"
                    print(error_msg)
                    #out6.write(f"Retry failed after {max_retries} attempts for ID: {identifier}.\n")
                    out6.write(error_msg)
                    out6.flush()
                    processed = True
                    break  # Exit retry loop and move to next record
                    
                    
                
                except requests.exceptions.HTTPError as err:
                    print("HTTP Error:")
                    print(err)
                        
            if not processed:
                print(f"Giving up on record nr {nr}. Moving to next record.\n")
            #return None  # Return a value indicating failure so the calling code can skip

    except BaseException as err:
        print("Base Exception error:")
        print(err)

def fetch_token():
    global token
    
    for attempt in range(max_retries):
        try:
            token = wskey.fetch_token(token_url='https://oauth.oclc.org/token', auth=auth, timeout=timeout_token)
            current_datetime = datetime.datetime.now()
            formatted_datetime_token = current_datetime.strftime("%H:%M:%S")
            if args.verbose:
                print(f"Fetched new token at {formatted_datetime_token}: {token}\n")
                print("----------------------------------------------------------------------------------------------------\n")
            return token
        except requests.exceptions.Timeout:
            print(f"Token request timed out for {institution}, retrying in 3 seconds... ({attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("Max retries reached for token request.")
                return None
        
        except Exception as e:
            print(f"Error fetching token for {institution}: {e}")
            return None
        
def token_refresher():
    if args.verbose:
        print("Fetching token for the first time...")
    while True:
        #time.sleep(10) # Sleep for 30 seconds
        time.sleep(18 * 60) # Sleep for 18 minutes
        if args.verbose:
            print("Token refresh!")
        fetch_token()
        if args.verbose:
            print("Refreshed token!")


if __name__ == '__main__':
    
    # Get the name of the script
    script_name = os.path.basename(__file__)[:-3]

    # Get the current date
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%y%m%d.%H%M%S")
    
    # Token refresher call
    token_thread = threading.Thread(target=token_refresher, daemon=True)
    token_thread.start()
    
    
    # Use glob to expand the wildcard pattern
    file_list = glob.glob(args.input_file)
    
    # Check if files were found
    if not file_list:
        raise FileNotFoundError(f"No files found matching the pattern: {args.input_file}")
        sys.exit(1)
            
    
    if args.mode == 'a':
        if all(os.path.splitext(file)[1] == '.mrc' for file in file_list):
            print(f"\n***Format file approved '.mrc'\n")
        else:
            print(f"\n!ERROR: All input files must be '.mrc'\n\nExiting program without execution...\n")
            sys.exit(1)
    

    # Select function to execute based on the second argument             
    if args.mode == 'a':        
        # Process each file found 
        for file_name in file_list:
            print("\n==================================================")
            print(f"-->Input file processed: {file_name}\n")

            # Output file paths
            base_name = os.path.splitext(os.path.basename(file_name))[0]
            outfile=f"{base_name}.{script_name}.{institution}.{formatted_datetime}"
            output1=f"{outfile}.ReplacedBIBs.mrc"
            output2=f"{outfile}.BadRequest.xml"
            output6=f"{outfile}.LOG.txt"

            main(file_name)

      
    print(f"\n***End of file***")
    print(f"***End of script***")
