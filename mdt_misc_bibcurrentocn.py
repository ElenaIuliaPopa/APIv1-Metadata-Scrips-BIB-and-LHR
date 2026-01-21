#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  Copyright (c) 2024 by OCLC
#
#  File		: mdt_misc_bibcurrentocn.py
#  Description	: Script to get the current OCNS from WorldCat
#  Author(s)	: Elena Popa
#  Creation	: 11-06-2024
#
#  History:
#  11-06-2024	: popae    : Creation
#  08-07-2024	: popae    : Added -> help menu '-h', '--help'
#                          : autoflush (buffering=1)
#                          : script Name & institution symbol in variables & in output files names
#                          : new line at the EOF
#                          : timeout & retry & timeout outfile -- 50s default for API 
#                          : processing 100 OCNs per call (instead of 1)
#                          : duration of execution
#                          : processing lists with 'PPN   OCN' (tab separated)
#  09-07-2024	: popae    : Added -> timeout output files 'PPN  OCN' 
#  10-07-2024	: popae    : Added -> token refresher every 19 mins. 
#  21-08-2024	: popae    : Mapping PPN OCN to the correct result from API working 
#                          : even if the the OCN delers are immed next to each other      
#                          : or in the same batch of 100    
#                          :     
#  02-12-2024              : Result is parsed as Json and data is extracted with json     
#                          : Api reponse has changed when record not found, updated to recognize the None as 'null'    
#                          :     
#                          :     
#                          :     
#
#  Notes	               :
#                          : 
#                          : 
#       
#  SVN ident	: $Id: mdt_misc_bibcurrentocn.py 413865 2024-09-24 13:05:32Z popae $

# Built-in/Generic Imports

import os
import sys
import argparse
import fnmatch
import threading  # to refresh the token

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
import tempfile
import gzip
import shutil

# Get the name of the script
script_name = os.path.basename(__file__)[:-3]

def print_help():
        help_text = f"""
    # ##################################################################################################
    #                                                                                                  #
    #                       USAGE:  {script_name}.py input_file                               #
    #                                                                                                  #
    # ##################################################################################################
    #                                                                                                  #
    # Script for retrieving Current OCNs from WorlddCat                                                #
    #                                                                                                  #
    # WSKey used for authorization: TS268                                                              #
    #                                                                                                  #
    # ##################################################################################################
    #                                                                                                  #
    # @ Mandatory argument: Input file                                                                 #
    #   Input_file is a .txt or .xml file containing:                                                  #
    #   - a list with 1 OCN per line                                                                   #
    #   - a list with PPN   OCN per line (tab separated)                                               #
    #   - xml files are transformed in txt files with PPN   OCN per line (tab separated)               #
    #                                                                                                  #
    #     -- the script processes 100 OCNs per call                                                    #
    #                                                                                                  #
    # =================================================================================================#
    #                                                                                                  #
    # @ Files generated:                                                                               #
    #                                                                                                  #
    #       - .not_found.ocns.txt     - list with OCN's that return in 'current' the value null        #
    #                                   REMOVED OCNS from WC                                           #
    #                                   preceded by PPN if input has it                                #
    #       - .equal.ocn.txt          - list with OCNS's found in WC with the same value in 001        #
    #                                   NOT CHANGED                                                    #
    #                                   preceded by PPN if input has it                                #
    #       - .changed.ocns.txt       - list with OCNS's found in WC but in 019 instead  of 001        #
    #                                   MERGED OCNS                                                    #
    #                                   preceded by PPN if input has it                                #
    #       - .all.ppn.ocns.txt       - list with ALL OCNS and Current OCNs where present              #
    #                                   preceded by PPN if input has it                                #
    #       - .timeout.request.txt    - list with OCNS for which the API request timed out             #
    #                                   preceded by PPN if input has it                                #
    #       - .timeout.token.txt      - list with OCNS for which the TOKEN request timed out           #
    #                                   preceded by PPN if input has it                                #
    #                                                                                                  #
    # =================================================================================================#
    #                                                                                                  #
    # - It uses the WorldCAT Metadata API 2.0:                                                         #  
    # https://developer.api.oclc.org/wc-metadata-v2?_gl=1*1okopt2*_gcl_au*MTM0NzM1MzgyNi4xNzE1Njg5Mjg3 #
    #                                                                                                  #
    # - Obtains a token with Client Credentials Grant: https://github.com/requests/requests-oauthlib   #
    #                                                                                                  #
    # - Request URL: https://metadata.api.oclc.org/worldcat/manage/bibs/current?oclcNumbers=811455535  #
    #              -H 'accept: application/json' \                                                     #
    #                                                                                                  #
    # - Token refreshed every 18 mins. Default expiry time is 20 mins                                  #
    #   - set on < 20 otherwise it expires before it manages to refresh                                #
    #                                                                                                  #
    # !!! To work on Windows or Unix it requires installation of:                                      #
    # - pip                                                                                            #
    # - oauthlib.oauth2                                                                                #
    # - requests.auth                                                                                  #
    # - requests_oauthlib                                                                              #
    #                                                                                                  #
    ####################################################################################################
    
    """
        
        return help_text


help_text = print_help()

# Custom Formatter for regular help text
class CustomHelpFormatter(argparse.RawTextHelpFormatter):
    def _split_lines(self, text, width):
        return text.splitlines()

# Main parser for the regular help
parser = argparse.ArgumentParser(
    description='Script for retrieving Current OCNs from WorldCat.',
    formatter_class=CustomHelpFormatter
)

parser._optionals.title = 'Options'  # Customize options title if desired        
parser._positionals.title = 'Mandatory arguments'  # Customize positionals title if desired

#Positional (mandatory) argument:
parser.add_argument('input_file', nargs='?', help='input file to be processed. Can be ".txt" or ".xml."\nCan have only OCNs or PPN OCN tab separated.\nXml files are transformed in txt files, tab separated.')

# Optional arguments
parser.add_argument('-morehelp', action='store_true', help='display extensive explanation about the script')
parser.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity; use as argument AFTER the input file.\nexample: mdt_misc_bibcurrentocn.py <input_file> -v')


# Parse the arguments
args = parser.parse_args()

# Check if '-morehelp' is specified
if args.morehelp:
    print(help_text)
    sys.exit()

if args.verbose:
    print("Verbose mode is ON\n")
    
# Ensure input_file is provided if -morehelp is not used
if args.input_file is None:
    parser.error("The following arguments are required: input_file. Type -h or --help or -morehelp for more information")

    
# If '-morehelp' is not specified, continue with the regular argument handling
input_arg = args.input_file
print(f"Processing file: {input_arg}")

    
# Get the current date
current_datetime = datetime.datetime.now()
formatted_datetime = current_datetime.strftime("%Y-%m-%d.%H.%M.%S")


#<===== CONFIGURATION ======>

#with open("../config.yml", 'r') as stream:
#    config = yaml.safe_load(stream)

#Example_config.yml     
#key: MYKEY
#secret: MYSECRET
#auth_url: https://oauth.oclc.org/auth
#token_url: https://oauth.oclc.org/token
#metadata_service_url: https://metadata.api.oclc.org/worldcat

    
# serviceURL = config.get('metadata_service_url')
serviceURL = 'https://metadata.api.oclc.org/worldcat'

# Scope
scope = ['WorldCatMetadataAPI:manage_bibs']

#<======== AUTHORIZATION from config==========>
#auth = HTTPBasicAuth(config.get('key'), config.get('secret'))

#<======== CLIENT from config ==========>
#client = BackendApplicationClient(client_id=config.get('key'), scope=scope)


#<======== AUTHORIZATION & CLIENT TS268==========>
institution = "TS268"
auth = HTTPBasicAuth('2v4FUhRjcmfCVFy8IQMq3HqFuGAzHJphOWZQOqnbM3dAaZxPJri5px1hokMERzM8nrO9TKi0ev2tBtr1', 'JYBPF982myHASX7MuyWwHw==')
client = BackendApplicationClient(client_id='2v4FUhRjcmfCVFy8IQMq3HqFuGAzHJphOWZQOqnbM3dAaZxPJri5px1hokMERzM8nrO9TKi0ev2tBtr1', scope=scope)

wskey = OAuth2Session(client=client)

# <=== Set the parameters for timeout and retry. Token has to be declared for the refresh option ====>
# <=== Token expires after 20 minutes. Script fails to make request to the API. Refresh routine below in the script integrated ====>
token = None
timeout_token = 50
#timeout_token = 0.1
timeout_request = 50   #default timeout for the API
#timeout_request = 0.1  #for testing the timeout
max_retries = 3
retry_delay = 3

# <======== INPUT ARGUMENT  ========>
input_file = sys.argv[1]

if fnmatch.fnmatch(input_file, '*.txt') or fnmatch.fnmatch(input_file, '*.xml'):
    print(f"\n-->Input file processed: {input_file}\n")
else:
    print(f"\n!ERROR:Input file must be '.txt' or '.xml'\n\nExiting program without execution...\n")
    sys.exit(1)

# <=== Define output files ===>    
outfile = f"{input_file[:-4]}.{script_name}.{institution}.{formatted_datetime}"
               
out_xmltrans = f"{outfile}.PPN_OCN.txt"
outfile_notfound = f"{outfile}.not_found.ocns.txt"                      
outfile_equal = f"{outfile}.equal.ocn.txt"
outfile_changed = f"{outfile}.changed.ocns.txt"
outfile_all = f"{outfile}.all.ppn.ocns.txt" 
outfile_timeout = f"{outfile}.timeout.request.txt" 
outfile_timeout_token = f"{outfile}.timeout.token.txt" 
outfile_apikey = f"{outfile}.retry.apikey.txt" 

main_exec_time = 0

# <=== Transform input file if input is XML. Used for GGC consistency checks ===>   
def transformxml(input_file):
    #extracted_data = []
    extracted_data = ""
    
    #Parse the XML file
    tree = ET.parse(input_file)
    root = tree.getroot() #<xtr> is the root element
    
    for record in root.findall('record'):
        ocn = record.find('ocn').text if record.find('ocn') is not None else None
        ppn = record.find('./localid/sb').text if record.find('./localid/sb') is not None else None
    
        # Debugging print statements to check what is being found
        #print(f"Found ocn: {ocn}", flush=True)
        #print(f"Found ppn: {ppn}", flush=True)
        #input("Press Enter to continue...")
        
        if ocn and ppn:
            #extracted_data.append(f"{sb}\t{ocn}\n")
            extracted_data += (f"{ppn}\t{ocn}\n")
    
    #print("Extracted Data:", flush=True)
    #print(extracted_data, flush=True)
    #input("Press Enter to continue...")
    return extracted_data
    
if fnmatch.fnmatch(input_file, '*.xml'):
    if args.verbose:
        print(f"Transforming XML file: {input_file}")
    
    extracted_data = transformxml(input_file)
    
    with open(out_xmltrans, 'w') as f:
        f.write(extracted_data)
        input_file = out_xmltrans

    
# <=== Read number of lines ===>    
line_count = 0
with open(input_file, "r") as file:
    lines = file.readlines()
    line_count = len(lines)
    print(f"Number of lines read: {line_count}\n\nProcessing 100 OCNs per request.\n\n")

# <=== Truncate input file in batches of 100 lines ===> 
def main(input_file):
    global request_nr
    #global token
    request_nr  = 0  


    token = fetch_token()
    if token is None:
        print("Fetching Token in main failed")
    
    else:    
        if args.verbose:
            print("Token in main fetched")
    
    
    with open(input_file, "r") as file:
        lines = file.readlines()    
    #print(f"Content of input file: {lines}")
    #input("Press Enter to continue...")

    for i in range(0, len(lines), 100):        
        batch_lines = lines[i:i+100]
        first_line = batch_lines[0].strip()
        ocn = first_line if first_line.isdigit() else first_line.split('\t')[1].strip()

        #print(f"OCN: {ocn}")
        #input("Press Enter to continue...")

        #print(f"Processing lines {i + 1} to {min(i + 100, len(lines))}:")
        #print(batch_lines)


        if token is None:
            request_nr += 1
            print(f"-->Skipping request nr[{request_nr - 1}] -- Starting line [{request_nr -1}01] --- for batch starting with number {ocn} due token fetch failure.")
            output_timeout_token(batch_lines)
            fetch_token()            
            continue
        
            # This exits the script if the token fetch failed
            #print("Failed to fetch token after maximum retires. Exiting.")
            #sys.exit(1)
    
        result, timeout_str = process(batch_lines)
        request_nr += 1

        if args.verbose:
            print(f"Result final sent back before compare: {result}")

                
        if result is None:
            print(f"-->Skipping request nr[{request_nr - 1}] -- Starting line [{request_nr -1}01] --- for batch starting with number {ocn} due to timeout.")
            if timeout_str:
                generate_timeout(batch_lines, timeout_str)
            continue
                        
        allocns = compare(result)

        generate_outfiles(batch_lines, allocns)
        
        if timeout_str:
            generate_timeout(batch_lines, timeout_str)
        
   

# <=== COMPARE RESULTS FROM WC:   ===>       
def compare(result):
    allocns = []
    
    #lines = result.splitlines()
    for line in result:
        values = line.split("|")
        requested = values[0]
        current = values[1].strip()
    
        if current == 'null':                                         
            allocns.append(requested + "\t" + "Record not found.\n")

        elif requested == current:
            allocns.append(requested + "\t" + "Record found.\n")

        elif requested != current:
            allocns.append(requested + "\t" + "Record changed." + "\t" + current + "\n")
               
    #print(allocns)
    if args.verbose:
        print("Compare results done\n")
    return allocns

# <=== GENERATE OUTPUT FILES AND MAP TO PPN:   ===>  
def generate_outfiles(batch_lines, allocns):
    
    notfound = []
    equal = []
    changed = []
    allocns1 = []
    
    #print("Allocns list:", allocns)
    allocns_dict = {line.split('\t')[0]: line for line in allocns}
    
    #print("Batch lines processed:")
    #print(batch_lines)

    for line in batch_lines:    
        input_line = line.strip()
        #print(input_line)
        ocn = input_line if input_line.isdigit() else input_line.split('\t')[1].strip()

        if ocn in allocns_dict:
            allocn_line = allocns_dict[ocn]
            if "Record not found" in allocn_line:                                       
                notfound.append(input_line + "\n")                                          
                allocns1.append(input_line + "\t" + "Record not found.\n")    
            elif "Record found." in allocn_line:                                          
                equal.append(input_line + "\n")                                          
                allocns1.append(input_line + "\t" + "Record found.\n")    
            elif "Record changed." in allocn_line:
                current = allocn_line.split('\t')[2].strip()                                       
                changed.append(input_line+ "\t" + current + "\n")                                          
                allocns1.append(input_line + "\t" + "Record found." + "\t" + current + "\n")
               
    if notfound:
        with open(outfile_notfound, "a", buffering=1) as file:
            file.writelines(notfound)
    if equal:
        with open(outfile_equal, "a", buffering=1) as f:
            f.writelines(equal)
    if changed:
        with open(outfile_changed, "a", buffering=1) as file:
            file.writelines(changed) 
    if allocns:
        with open(outfile_all, "a", buffering=1) as file:
            file.writelines(allocns1)

    if args.verbose:
        print("Output written to output files\n")
          
# <=== Generate the timeout request output file ==>           
def generate_timeout(batch_lines, timeout_str):
    
    print("-->Logging timeouts and moving to the next 100 OCNS.\n")
    print("----------------------------------------------------------------------------------------------------\n")
    timeout_dict = {line: line for line in timeout_str.splitlines()}
    timeout = []
    
    for line in batch_lines:    
        input_line = line.strip()
        ocn = input_line if input_line.isdigit() else input_line.split('\t')[1].strip()    
       
        if ocn in timeout_dict:
            timeout.append(input_line + "\n")
            
    if timeout:
        with open(outfile_timeout, "a", buffering=1) as f:
            f.writelines(timeout)

    if args.verbose:
        print("Timeout request OCNs written to outputfile.\n")
        
# <=== Generate the timeout token output file ==>           
def output_timeout_token(batch_lines):
    
    print("-->Logging timeouts and moving to the next 100 OCNS.\n")
    print("----------------------------------------------------------------------------------------------------\n")
    
    with open(outfile_timeout_token, "a", buffering=1) as f:
            f.writelines(batch_lines)
   
    if args.verbose:
        print("Timeout token OCNs written to outputfile.\n")
        
# <===== Obtain a token and authorize =====>

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

        
# <===== Building OCNs list and sending the request to API, fetching results for 100 OCNs =====>            
def process(batch_lines):    
    global request_nr 
    global main_exec_time    
    main_start = time.time()
    
    numbers = []
    try:                
        for line in batch_lines:
            line = line.strip()
            if '\t' in line:   # If line is PPN      OCN
                parts = line.split('\t')
                ppn = parts[0]
                ocn = parts[1].strip()
                numbers.append(ocn)
            elif line.isdigit():    # If line is only OCN
                numbers = [line.strip() for line in batch_lines]
            else:
                print(f"!ERROR: Input file must be tab-separated or OCNs only.\n!ERROR: Unknown separator in input provided:\n\t{line}\n\nExiting program...\n")                    
                sys.exit(1)

        timeout_str = None
        oclc_numbers = '&oclcNumbers='.join(numbers)

        ## -------------------------------------------------------------------------------------------------------------------- ##     
             
        def request_data(oclc_numbers):
            r = wskey.get(serviceURL + "/manage/bibs/current?oclcNumbers={}".format(oclc_numbers), headers={"accept":"application/json"}, timeout=timeout_request)     
            url_sent = serviceURL + "/manage/bibs/current?oclcNumbers={}".format(oclc_numbers)
            
            if args.verbose:
                print(f"-----------------------------------------------------------------------------")
                print(f"Request number [{request_nr}]-- Starting line [{request_nr}01] -- Processing batch starting with number [{numbers[0]}]:\n")
            
            r.raise_for_status
            result = r.content
            result = result.decode('utf-8')   #Data from the API is returned as bytes.
            result_json = json.loads(result)  # Parse the API response                          
            
            
            if args.verbose:
                print(f"Result:\n{result}")
                input("Press Enter to continue...")

            return result_json
        ## -------------------------------------------------------------------------------------------------------------------- ##    

        for attempt in range(max_retries):                                
            try:                                                    
                result = request_data(oclc_numbers) # This is the JSON result

                #result comes from the routine back:
                if 'API rate limit exceeded' in result:
                    print(f"API rate limit exceeded.Exiting program...\n")
                    sys.exit(0)
                
                elif (
                            'API Key or Authorization header is required' in result or 
                            '<!DOCTYPE html>' in result or  
                            '<head><title>502 Bad Gateway</title></head>' in result
                    ):

                    if args.verbose:
                        print("Fetching new token")
                    
                    fetch_token()

                    if args.verbose:
                        print("Token fetched in retry")
                    
                    print(f"Retrying the request [{request_nr}]:")

                    with open(outfile_apikey, "a", buffering=1) as f:
                        f.writelines(f"{result} : Request [{request_nr}] has been retried.\n")

                    continue # Go to next attempt to retry

                #If no error found, proceed
                if "controlNumbers"  in result:
                    controlNumbers = result['controlNumbers']
                    result_final = []

                    for controlNumber in controlNumbers:
                        requested = controlNumber['requested']
                        current = controlNumber['current']

                        if args.verbose:
                            print(f"Requested: {requested}, Current: {current}")

                        if current is None:
                            current = 'null'

                        result_final.append(requested + "|" + current + "\n")

                        if args.verbose:
                            print("Results sent DIRECTLY to compare.")
                    
                    
                else:
                    #print(f"-----------------------------------------------------------------------------")
                    print("Error: OCNS not fetched. Response from the API:")
                    print(result)
                

                return result_final, timeout_str
        
            except requests.exceptions.Timeout:
                print(f"Request number [{request_nr}] -- Starting line [{request_nr}01] --- timed out for batch starting with number {numbers[0]}, retrying in 3 seconds... ({attempt + 1}/{max_retries})")
                if attempt < max_retries - 1 :
                    time.sleep(retry_delay)
                else:
                    timeout_str = oclc_numbers.replace('&oclcNumbers=', '\n')
                    print(f"-->Batch failed after {max_retries} attempts.")
                    return None, timeout_str
                        
            except requests.exceptions.HTTPError as err:
                print(err)
                break                
            except BaseException as err:
                print(err)
                break
        
        return None, timeout_str
        
    except FileNotFoundError:   
        print(" Input file not found.")
        return None, None
    except Exception as e:
        print(" An error occurred:", e)
        return None, None
        
    main_end = time.time()
    

# <==== Record start time of the script ====>
start_time = time.time()
        
# <==== Call the main function ====>
if __name__ == '__main__':
    
    token_thread = threading.Thread(target=token_refresher, daemon=True)
    #token_thread = threading.Thread(target=token_refresher)
    token_thread.start()
    
    main(input_file)
    #token_thread.join()


"""    
# <===== Add new line at EOF for Linux files ====>
def add_line_eof(file_names):    
    for file_name in file_names:
        if os.path.exists(file_name) and os.path.getsize(file_name) > 0:
            with open(file_name, 'a') as f:
                f.write('\n') 

#outfile_list = [outfile_notfound, outfile_equal, outfile_changed, outfile_all, outfile_timeout, outfile_notfound_ppn, outfile_equal_ppn, outfile_changed_ppn, outfile_all_ppn, outfile_timeout, outfile_timeout_ppn]
outfile_list = [outfile_notfound, outfile_equal, outfile_changed, outfile_all, outfile_timeout, outfile_timeout_token, outfile_apikey]
add_line_eof(outfile_list)
"""


# <==== Record end time of execution ====>
end_time = time.time()
execution_time = end_time - start_time
execution_time_mins = execution_time / 60
execution_time_hours = execution_time_mins / 60

if execution_time < 60:
    print(f"End of execution. Duration: {execution_time:.2f} seconds")
elif execution_time < 3600:    
    print(f"End of execution. Duration: {execution_time_mins:.2f} minutes")
else:    
    print(f"End of execution. Duration: {execution_time_mins:.2f} minutes")

#=======================================================================       
#Example response json:
#{
#  "controlNumbers": [
#    {
#      "requested": "1",
#      "current": "1"
#    },
#    {
#      "requested": "261176486",
#      "current": "311684437"
#    },
#    {
#      "requested": "999999999999",
#      "current": null
#    }
#  ]
#}  


#Example response text:
#{"controlNumbers":[{"requested":"65796010","current":"65796010"}]}
#{"controlNumbers":[{"requested":"261176486","current":"311684437"}]}
#{"controlNumbers":[{"requested":"999999999999","current":null}]}

# <=== CLEAN OUTPUT FILE. DESIRED RESULT:   ===>
#  261176486|311684437
#  999999999999|null

"""
# Multiple OCNs request:
https://metadata.api.oclc.org/worldcat/manage/bibs/current?oclcNumbers=898993466&oclcNumbers=798550086&oclcNumbers=798550087&oclcNumbers=65629121&oclcNumbers=65796010&oclcNumbers=261176486&oclcNumbers=999999999999

{"controlNumbers":[{"requested":"798550086","current":"798550086"},{"requested":"798550087","current":"798550087"},{"requested":"65629121","current":"65629121"},{"requested":"999999999999","current":null},{"requested":"898993466","current":"898993466"},{"requested":"261176486","current":"311684437"},{"requested":"65796010","current":"65796010"}]}

"""
        




