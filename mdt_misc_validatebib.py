#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Copyright (c) 2025 by OCLC
#
#  File		    : mdt_misc_validatebib.py
#  Description	: Script to validate Marc21 records through the MAPIv2
#  Author(s)	: Elena-Iulia Popa
#  Creation	    : 06-01-2025
#
#  History:
#  06-01-2025	: popae : Creation
#  07-01-2025   : popae : Added Help Menu and verbose option
#
#  Notes	    :
#               : 
#               : popae : 'API Key or Authorization header is required' set to retry 3 times
#               : popae : Input files can be: - 1 mrc with 1 record / multiple mrc with 1 records each
#               :                             - 1 mrc with more records / more mrc with more records each
#               : 
#               : poape: Added functions: -i (path to input file) -o (path to output directory) -m (mode) -v (verbose)
#               : popae: Output directory ensured if it does not exist
#               : popae: File names handled with built function to remove extension
#               : popae: Format date shorter
#               : 
#
#  SVN ident	: $Id$

# Built-in/Generic Imports

import os
import sys
import argparse
import fnmatch
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

#-----------------------------------------------------------------------------
# Main parser for the regular help
parser = argparse.ArgumentParser(
    description='Script to validate BIB records through the MAPIv2' ,
    formatter_class=argparse.RawTextHelpFormatter  # # This prevents argparse from reformatting help text
)

parser._optionals.title = 'Options' #Customize options title if desired
parser._positionals.title = 'Mandatory arguments' #Customize positionals title if desired

# Positional (mandatory) argument:
parser.add_argument("-i", "--in", dest="input_file", required=True, help=(
                                                'Path to input file to be processed or path to "file_pattern".\n'
                                                '- If Input file is in the working directory, just the file name is enough.\n'
                                                '- "file_pattern" requires double quotes.\n'
                                                "- Input file should be .mrc if -m v[alidate] or -m f[ull].\n"
                                                "- It should be .json if -m r[esult]."
 )
)

parser.add_argument('-o', '--output',   default='.',  help =(
                                                "Optional path to the output directory (default: working directory).\n"
                                                "- If output directory does not exist (not full path, just last directory), it will be created.\n"
                                                "- If output file does not exist, it will be created.\n"
 )
)

parser.add_argument('-m', '--mode',  nargs="?", const='f', default=None, required=True, choices=['v', 'f', 'r'], help =(
                                                "'[v]alidate' or '[r]esult' or '[f]ull'.\n"
                                                "- If no choice  is provided, the default is '[f]ull'.\n"
                                                "- v = input should be .mrc file. It will generate only the .json API response file.\n"
                                                "- r = input is the .json API response. It will generate the stats and reports.\n"
                                                "- f = input should be .mrc file. It will do both the validate and the stats, reports."
 )
)


# Optional arguments
parser.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity; use as argumnet AFTER the input file.')

# Parse the arguments
args = parser.parse_args()

input_path = os.path.abspath(args.input_file)
output_path = os.path.abspath(args.output)

# Create output directory if it doesn't exist
if not os.path.isdir(output_path):
    os.makedirs(output_path, exist_ok=True)


#print("Input path:", input_path)
print("Output directory ensured:", output_path)
#sys.exit(0)

''' DEBUG -------------------------------
#input_arg = args.input_file
#file_list = glob.glob(args.input_file)

print("Input path: ", input_path)
print("Output directory:", output_path)
print("File list:", file_list)
print("File list:", file_list[0])

# Result:
# Input file:  /users/develop/popae/work/WMS/Heidelberg/July_Export_Validate/mrc/TEST_script_Validate/*.mrc
# Output directory: /home/popae/work/WMS/Heidelberg/July_Export_Validate/mrc/01
# File list: ['2011_2008-tit.001.mrc', '2011_2006-tit.002.mrc', '2011_1999-tit.003.mrc']
# File list: 2011_2008-tit.001.mrc

sys.exit(0)
'''

if args.verbose:
    print("Verbose mode is ON.\n")

# Ensure input_file is provided if -morehelp is not used
if args.input_file is None:
    parser.error("The following arguments are required: input_file. Type -h or --help or -morehelp for more information")

input_arg = args.input_file
print("========================================")
print(f"***Processing file(s): {input_arg}")
#sys.exit(0)
#-----------------------------------------------------------------------------
"""===============ENVIRONMENTS=================="""
# serviceURL = config.get('metadata_service_url')

#PRODUCTION
serviceURL = 'https://metadata.api.oclc.org/worldcat'

#DEV
#serviceURL = 'https://metadata-api-m1.dev.oclc.org/worldcat'

#WORKS
#serviceURL = 'https://int.metadata.api.oclc.org/worldcat'

#INT
#serviceURL = 'https://metadata-api-m1.nat.oclc.org/worldcat'

"""===============SCOPE=================="""
scope = ['WorldCatMetadataAPI:manage_bibs', 'WorldCatMetadataAPI:view_brief_bib']


"""<======== AUTHORIZATION & CLIENT ==========>"""
institution = "TS268"
auth = HTTPBasicAuth('2v4FUhRjcmfCVFy8IQMq3HqFuGAzHJphOWZQOqnbM3dAaZxPJri5px1hokMERzM8nrO9TKi0ev2tBtr1', 'JYBPF982myHASX7MuyWwHw==')
client = BackendApplicationClient(client_id='2v4FUhRjcmfCVFy8IQMq3HqFuGAzHJphOWZQOqnbM3dAaZxPJri5px1hokMERzM8nrO9TKi0ev2tBtr1', scope=scope)

"""<======== END LIST ==========>"""

#-----------------------------------------------------------------------------
wskey = OAuth2Session(client=client)

token = None
max_retries = 3
timeout_token = 50
timeout_request = 50   #default timeout for the API
max_retries = 10
retry_delay = 3


#-----------------------------------------------------------------------------
# The all responses from API are sent here
# Written as a single JSON file with the list of all the API responses
def writefile(all_records_json, output_path):
    
    if all_records_json is None:
        print(f"Warning: Tried to write None result. Exiting program...")
        return
    
    final_json = {"records": all_records_json}
    with open(output_path, 'a', encoding='utf-8', buffering=1) as out1:
        json.dump(final_json, out1, indent=2, ensure_ascii=False)
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Comes with each file , send to API, makes list of all the API responses
# Returns the list to main
# it goes then to writefile
def main(file_name):
    
    token = fetch_token()
    if token is None:
        print("Fetching token in main failed")
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
    all_records_json = []
  
    for record in records:
        #print(f"This is record: {record}")
        #input(...)
        result_json, records_count, nr = process_record(record, nr, records_count)
        all_records_json.append(result_json)
        
        #print(f"API response sends to main:\n {modified_result}\n")
    
    return all_records_json
            
#-----------------------------------------------------------------------------    
def process_record(record, nr, records_count):    
    #print("I am here 1")
    try:
        #print("I am here 2")
        nr += 1
        split_record = record.split(b'\x1E') # RS split
        #print("I am here 3")

        # Get an identifier
        if len(split_record) > 1:
            #print("I am here 4")
            # NO LSN in 001 option and No 029
            #identifier = re.search(rb'\x1Fa\d+', record).group().decode("UTF-8") #searching for 1st string of digits in $a in mrc record , whatever that is
            #identifier = identifier.replace('\x1Fa','') # removes the $a
            
            # With LSN in 001 option
            identifier = split_record[1]
            identifier = identifier.decode("UTF-8")
            
            # Heidelberg: =029  0\$aGBVCP$b027096580    # from the .mrk file   
            #\x1e0 \x1faGBVCP\x1fb027096580\x1e # from the .mrc file
            '''
            pattern = rb'0 \x1faGBVCP\x1fb(\d+X?)$'
            for field in split_record:
                match = re.search(pattern, field)
                if match:
                    identifier = match.group(1).decode("UTF-8")
            '''    
            #identifier = re.search(rb'\x1Fa\(DE-627\)\d+X?', record).group().decode("UTF-8")
            #identifier = identifier.replace('\x1Fa(DE-627)','') # removes the $a
            
            if args.verbose:
                print(f"Identifier: {identifier}\n")
                
            
            #print("I am here 5")
            def request_data(record):
                #print("I am here 6")

                r = wskey.post(serviceURL + "/manage/bibs/validate/validateFull", data=record, headers={"Content-Type":"application/marc"})  
                #r.raise_for_status does nothing without ()
                
                #r.raise_for_status()  # returns ==>:
                #HTTP Error:
                #400 Client Error:  for url: https://metadata.api.oclc.org/worldcat/manage/bibs/validate/validateFull
                result = r.content
                result = result.decode("UTF-8")
                result_json = json.loads(result)  # Parse the API response
                
                return result_json
                   
            
            with open(output6, 'w', buffering=1) as out6:
                for attempt in range(max_retries): 
                    try:
                        # Send the record to the API Request
                        result_json = request_data(record)
                        
                        if 'API rate limit exceeded' in result_json:
                            print(f"{identifier}|API rate limit exceeded.Exiting program...\n")
                            sys.exit(0)
                        
                        elif (
                            'API Key or Authorization header is required' in result_json or 
                            '<!DOCTYPE html>' in result_json or  
                            '<head><title>502 Bad Gateway</title></head>' in result_json
                         ):
                            
                            #out6.write(f"{identifier}:\n {result}\n")
                            out6.write(f"Attempt {attempt+1} - Error for Identifier {identifier}:\n{result_json}\n")
                            out6.flush()
                            
                            print(f'Error encountered for Identifier: {identifier}:\n***{result_json}***')
                            print(f'Retrying request for Identifier: {identifier}\n')
                            
                            # Refresh token and try again
                            fetch_token()
                            #result = request_data(record) This only sends to function, does not check for error again, it assume succes
                            continue  # Go to next attempt to retry
                            
                            
                        # If no error found, proceed
                        if args.verbose:                                
                            print(f"Success for Identifier: {identifier}\n")
                        
                        result_json["IDENTIFIER"] = identifier  # Add identifier as a field

                
                        if args.verbose:
                           print(f"Modified result:\n{result_json}\n")
                           
                        #break  #return function exits the loop anyway;
                               
                        #print(f"Modified_result = {modified_result}\n Records_count = {records_count}\n, Nr = {nr}\n")
                        return result_json, records_count, nr

                    except RetryError:
                        #print(f"-->Retry failed after {max_retries} attempts.")
                        error_msg = f"--> Retry failed after {max_retries} attempts for ID: {identifier}.\n"
                        print(error_msg)
                        #out6.write(f"Retry failed after {max_retries} attempts for ID: {identifier}.\n")
                        out6.write(error_msg)
                        out6.flush()
                        break  # Exit retry loop and move to next record
                        
                        
                    
                    except requests.exceptions.HTTPError as err:
                        print("HTTP Error:")
                        print(err)
                        
                print(f"Giving up on Identifier: {identifier}. Moving to next record.\n")
                return None, records_count, nr  # Return a value indicating failure so the calling code can skip        
    except BaseException as err:
        print("Base Exception error:")
        print(err)


#-----------------------------------------------------------------------------        
def create_report(file_name):

    if not file_name:
        raise ValueError("File name is empty or None. Please provide valid file name.")
    
    """
    else:
        print(f"Type of file_name: {type(file_name)}")
        print(f"Content of file_name: {file_name[:100]}")
    """

    out3 = []
    out_csv = []
    out2_content = []
    out5_content = []
    
    #Parse the JSON
    with open(file_name, 'r', encoding='UTF-8') as file:
        file_content = file.read()

    try:
        data = json.loads(file_content)
        if args.verbose:
            print("JSON structure:")
            print(json.dumps(data, indent=2)) # Print full Json
        if args.verbose:
            print(f"Data loaded: \n {data}")
    except json.JSONDecodeError as e:
        print(f"The variable is not valid JSON. Error: {e}")
        return
    
    
    #{"IDENTIFIER" : "1090238908",
    #"message":"API rate limit exceeded"},
    
    # Process each record in the "records" list:
    records = data.get("records",[])
    for record in records:
        identifier = record.get("IDENTIFIER")
        api_msg = record.get("message")
        
        if args.verbose:
            print(f"API msg: {api_msg}")
       
        if args.verbose:
            print(f"Record ID: {identifier}")
   
        description = record.get("status", {}).get("description")
        if args.verbose:
            print(f"Description: {description}\n")
    
    
        # Get and parse the validation errors nest
        # validationErrors is a dictionary that contains more nested structures
        validation_errors = record.get("validationErrors", {}) 
        #print(f"Validation Errors: {validation_errors}")
        #print(f"Type of validation_errors: {type(validation_errors)}") 
    
        # Extract individual error types - these are LISTS
        error_count = validation_errors.get( "errorCount")
        error_summary = validation_errors.get( "errors", [])
        record_errors = validation_errors.get( "recordLevelErrors", [])
        fixedfield_errors = validation_errors.get( "fixedFieldErrors", [])
        #print(f"Type of fixedfield_errors: {type(fixedfield_errors)}")
        varfield_errors = validation_errors.get( "variableFieldErrors", [])
    
        # Combine all error types into one list that you can iterate through
        all_errors = record_errors + fixedfield_errors + varfield_errors
           
        
        #if 'API rate limit exceeded' in api_msg:
        if api_msg:
            if 'API rate limit exceeded' in api_msg:
                #out5_content.append(f"{identifier}|{api_msg}\n")
                print(f"{identifier}|{api_msg}\n")
                
        elif description:            
            if 'The provided Bib is invalid' in description:
                #print(f"I am here 1\n")
                #input(...)
                #print(f"Error count: {error_count}\n")
                #print(f"Error summary: {error_summary}\n")
                
                # Check for the special "001 must be present." case
                if int(error_count) == 1 and "001 must be present." in error_summary:
                    #print(f"Message for 001 only: {error_summary}\n")
                    #input(...)
                    out5_content.append(f"{identifier}|{error_summary[0]}\n")
                
                else:
                    for error in all_errors:           
                        tag = error.get("tag")
                        errorlevel = error.get("errorLevel")
                        message = error.get('message')

                        tagline = f"{errorlevel}|{tag}|{message}"
                        csv_line = f'"{errorlevel}"\t"{message}"'

                        out2_content.append(f"{identifier}|{tagline}\n")
                        out3.append(f"{tagline}\n")
                        out_csv.append(f"{csv_line}\n")
            else:
                out5_content.append(f"{identifier}|{description}\n")
            

    return out3, out5_content, out2_content, out_csv
    
#-----------------------------------------------------------------------------
def stats(out3, out_csv):
    
    # THis is what sits in out3
    # tagline = f"{errorlevel}|{tag}|{message}"
    line_counts = {}
    linec_counts = {}
    
    for line in out3:
        line = line.strip()
        if line in line_counts:
            line_counts[line] += 1
        else:
            line_counts[line] = 1
    
    with open(output4, 'w', encoding='UTF-8', buffering=1) as out4:
        for line, count in sorted(line_counts.items(),key=lambda x: (-x[1], x[0])):
            out4.write(f"{count}|{line}\n")
            
    for linec in out_csv:
        linec = linec.strip()
        if linec in linec_counts:
            linec_counts[linec] += 1
        else:
            linec_counts[linec] = 1
    
    with open(output7, 'w', encoding='UTF-8', buffering=1) as out7:
        out7.write(f'"ErrorOccurrence"\t"ErrorLevel"\t"ErrorMessage"\n')
        for linec, count in sorted(linec_counts.items(),key=lambda x: (-x[1], x[0])):
            out7.write(f'"{count}"\t{linec}\n')
            
#-----------------------------------------------------------------------------
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

#-----------------------------------------------------------------------------  
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


#====================END OF FUNCTIONS=================================>
"""# <==== Record start time of execution ====>"""
start_time = time.time()

# Convert to datetime object
dt_object = datetime.datetime.fromtimestamp(start_time)
print(f"Start time: {dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')}")


#============================================================>
"""# <==== MAIN LOGIC ====>"""
if __name__ == '__main__':
    
    # Get the name of the script
    script_name = os.path.basename(__file__)[:-3]

    # Get the current date for files name
    current_datetime = datetime.datetime.now()
    #formatted_datetime = current_datetime.strftime("%Y-%m-%d.%H.%M.%S")
    formatted_datetime = current_datetime.strftime("%y%m%d.%H%M%S")
    
    # Token refresher call
    token_thread = threading.Thread(target=token_refresher, daemon=True)
    token_thread.start()
    
    # Use glob to expand the wildcard pattern
    file_list = glob.glob(args.input_file)
    
    
    # Check if files were found
    if not file_list:
        raise FileNotFoundError(f"No files found matching the pattern: {args.input_file}")
        #print(f"No files found matching the pattern: {args.input_file}")
        #sys.exit(1)
            
    
    #args.mode ==> taken from the long option name (like --mode), unless you override it with dest=.
    if args.mode == 'f' or args.mode == 'v':
        if all(os.path.splitext(file)[1] == '.mrc' for file in file_list):
            print(f"\n***Format file approved '.mrc'\n")
            print("***Identifier will be the first digit string in the mrc record\nHeidelberg will be PPN in 029$aGBVCP")
        else:
            print(f"\n!ERROR: All input files must be '.mrc'\n\nExiting program without execution...\n")
            sys.exit(1)
    
    elif args.mode == 'r':
        if all(os.path.splitext(file)[1] == '.json' for file in file_list):
            print(f"\n***Format file approved '.json'\n")
        else:
            print(f"\n!ERROR: All input files must be '.json'\n\nExiting program without execution...\n")
            sys.exit(1)


    all_out3 = []
    all_out2 = []
    all_out5 = []
    all_out_csv = []
    
    # Define output file name depending on how many input files are processed
    # Clean the files of script and symbol if they are present
    # Especially for the result mode

        
    if args.mode == 'r':
        base_name = os.path.splitext(os.path.basename(file_list[0]))[0]
        base_name = base_name.replace("ValidationResponse", "").replace(".mdt_misc_validatebib.TS268", "")
        base_name = re.sub(r"\.\d{6}\.\d{6}\.", "", base_name)
        outfile_rest = f"{base_name}.{script_name}.{institution}.{formatted_datetime}"

    else:
        base_name = os.path.splitext(os.path.basename(file_list[0]))[0]
        outfile_rest = f"{base_name}.{script_name}.{institution}.{formatted_datetime}"
    
    #output6 = f"{outfile_rest}.LOG.txt"
    output6 = os.path.join(output_path, f"{outfile_rest}.LOG.txt")

    # This will generate the Json files for both full and validate
    if args.mode == 'f' or args.mode == 'v':
        # Process each file found 
        for file_name in file_list:
            print("\n==================================================")
            print(f"-->Input file processed: {file_name}\n")

            base_name = os.path.splitext(os.path.basename(file_name))[0]
            outfile=f"{base_name}.{script_name}.{institution}.{formatted_datetime}"
            #output1=f"{outfile}.ValidationResponse.json"
            output1 = os.path.join(output_path, f"{outfile}.ValidationResponse.json")

                
            # Obtain the json response from API from one file
            all_records_json = main(file_name)

            # Write the json response to the output file for each file
            # Here is one file with all the responses from the API for one file
            # output1 is returned from writefile function
            writefile(all_records_json, output1)
                
            # For full run send each output1 to create report and add values to the global lists
            if args.mode == 'f':
                # Create report for one file
                out3, out5_content, out2_content, out_csv = create_report(output1)        

                # Add values to the global lists
                all_out3.extend(out3)
                all_out5.extend(out5_content)
                all_out2.extend(out2_content)
                all_out_csv.extend(out_csv)

 
    # Once the API Json has been written, we can create the report
    # We have one or multiple json files
    # It should be able to go json by json and gather results
    # Will fill the global lists with the results that will be used to generate the reports and stats
    elif args.mode == 'r':
        for file_name in file_list:
            print("\n==================================================")
            print(f"-->Input file processed: {file_name}\n")        

            # Process the json result file
            # It needs to read the file name as json data and send that one to the create_report function
            
            out3, out5_content, out2_content, out_csv = create_report(file_name)

            # Add values to the global lists
            all_out3.extend(out3)
            all_out5.extend(out5_content)
            all_out2.extend(out2_content)
            all_out_csv.extend(out_csv)

               

    """Reports and Stats"""
    if args.mode == 'f' or args.mode == 'r':
        # Create report 
        #output2=f"{outfile_rest}.ALL.ValidationReport.txt"
        output2 = os.path.join(output_path, f"{outfile_rest}.ALL.ValidationReport.txt")
        with open(output2, 'w', encoding='UTF-8') as out2:
            out2.writelines(all_out2)
        
        #output4=f"{outfile_rest}.ALL.ValidationStats.txt"
        output4 = os.path.join(output_path, f"{outfile_rest}.ALL.ValidationStats.txt")
        #output7=f"{outfile_rest}.ALL.ValidationCSV.csv"
        output7 = os.path.join(output_path, f"{outfile_rest}.ALL.ValidationCSV.csv")
        # Send the values to the STATS function
        stats(all_out3, all_out_csv)

    
    
    print(f"\n***End of file***")
    
    """Valid Bibs"""
    if args.mode == 'f' or args.mode == 'r':
        # Write valid bibs only if found

        #output5=f"{outfile_rest}.ALL.ValidBibs.txt"
        output5 = os.path.join(output_path, f"{outfile_rest}.ALL.ValidBibs.txt")
        if all_out5:
            with open(output5, 'w', encoding='UTF-8') as out5:
                out5.writelines(all_out5)
        else:
            print("\nNo valid BIBs returned\n")
        

    print(f"***End of script***")

#==========================EXITING MAIN============================================
"""# <==== Record end time of execution ====>"""
end_time = time.time()

# Convert to datetime object
dt_object = datetime.datetime.fromtimestamp(end_time)
print(f"End time: {dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')}")


"""# <==== Print duration of execution ====>"""
execution_time = end_time - start_time

if execution_time < 60:
    seconds = int(execution_time)
    milliseconds = int((execution_time - seconds) * 1000)
    print(f"End of execution. Duration: {seconds} seconds and {milliseconds} milliseconds")
elif execution_time < 3600:
    minutes = int(execution_time // 60)  # integer division for minutes
    seconds = int(execution_time % 60)   # remainder seconds
    print(f"End of execution. Duration: {minutes} minutes and {seconds} seconds")
else:
    hours = int(execution_time // 3600)
    remainder = execution_time % 3600
    minutes = int(remainder // 60)
    seconds = int(remainder % 60)
    print(f"End of execution. Duration: {hours} hours, {minutes} minutes and {seconds} seconds")



#===========================END OF SCRIPT========================================#    
"""
FULL ERROR DISPLAY
html>
<head><title>502 Bad Gateway</title></head>
<body>
<center><h1>502 Bad Gateway</h1></center>
<hr><center>cloudflare</center>
</body>
</html>
,
"""