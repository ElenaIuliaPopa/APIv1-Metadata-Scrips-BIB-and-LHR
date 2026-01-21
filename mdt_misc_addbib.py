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
#  Notes	:
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

# Main parser for the regular help
parser = argparse.ArgumentParser(
    description='Script to add/create records in WC with MAPIv2. It uses TS268 auth.' ,
    formatter_class=argparse.RawTextHelpFormatter  # # This prevents argparse from reformatting help text
)

parser._optionals.title = 'Options' #Customize options title if desired
parser._positionals.title = 'Mandatory arguments' #Customize positionals title if desired

# Positional (mandatory) argument:
parser.add_argument("file_pattern", nargs="?", help=(
                                                'Input file to be processed or "file_pattern".\n'
                                                '- "file_pattern" requires double quotes.\n'
                                                "- Input file should be .mrc if function=validate or function=full.\n"
                                                "- It should be .json if function=result."
 )
)

parser.add_argument("function", nargs="?", help=(
                                                "'add' or 'result' or 'full'.\n"
                                                "- add  = input should be .mrc file. It will add and create a response file.\n"
                                                "- result = input is the .json API response. It will generate the stats and reports.\n"
                                                "- full   = input should be .mrc file. It will do both the validate and the reports."
 )
)

# Optional arguments
parser.add_argument('-v', '--verbose', action='store_true', help='increase output verbosity; use as argumnet AFTER the input file.')

# Parse the arguments
args = parser.parse_args()

if args.verbose:
    print("Verbose mode is ON.\n")

# Ensure input_file is provided if -morehelp is not used
if args.file_pattern is None:
    parser.error("The following arguments are required: file_pattern. Type -h or --help or -morehelp for more information")

input_arg = args.file_pattern
print("========================================")
print(f"***Processing file(s): {input_arg}")

# serviceURL = config.get('metadata_service_url')
serviceURL = 'https://metadata.api.oclc.org/worldcat'

# Scope
scope = ['WorldCatMetadataAPI:manage_bibs', 'WorldCatMetadataAPI:view_brief_bib']


#<======== AUTHORIZATION & CLIENT TS268==========>
institution = "TS268"
auth = HTTPBasicAuth('2v4FUhRjcmfCVFy8IQMq3HqFuGAzHJphOWZQOqnbM3dAaZxPJri5px1hokMERzM8nrO9TKi0ev2tBtr1', 'JYBPF982myHASX7MuyWwHw==')
client = BackendApplicationClient(client_id='2v4FUhRjcmfCVFy8IQMq3HqFuGAzHJphOWZQOqnbM3dAaZxPJri5px1hokMERzM8nrO9TKi0ev2tBtr1', scope=scope)

wskey = OAuth2Session(client=client)

token = None
max_retries = 3
timeout_token = 50
timeout_request = 50   #default timeout for the API
max_retries = 10
retry_delay = 3

is_first_line = True

def writefile(result, records_count, nr):
    global is_first_line
    
    with open(output1, 'a', encoding='utf-8', buffering=1) as out1:
        out1.write(f'{result}')
    
    """
    with open(output1, 'a', encoding='utf-8', buffering=1) as out1:
        if is_first_line:
            out1.write(f'{modified_result}')  # No comma as it is the first element
            if args.verbose:
                print(f"[{nr}]This was first line.\n")
                print(f"[{nr}]Result appended to output file.")
            is_first_line = False
        
        else:
            out1.write(f",\n\n{modified_result}") # Add modified result with comma before the "element"; This way no comma is added after the last element
            if args.verbose:
                print(f"[{nr}]Result appended to output file.\n")
    """

    #print(f"output1 is:\n {output1}\n")
    return output1

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
  
    for record in records:
        result, records_count, nr = process_record(record, nr, records_count)
        #print(f"API sends to main:\n {modified_result}\n")
        writefile(result, records_count, nr)
            
    
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
            # NO LSN in 001 option
            #identifier = re.search(rb'\x1Fa\d+', record).group().decode("UTF-8") #searching for 1st string of digits in $a in mrc record , whatever that is
            #identifier = identifier.replace('\x1Fa','') # removes the $a
            
            # With LSN in 001 option
            #identifier = split_record[1]
            #identifier = identifier.decode("UTF-8")
            
            # Heidelberg: =029  \\$a(DE-627)120923297
            #029 0 aTS268b1116241692
            identifier = re.search(rb'\x1FaTS268\x1Fb\d+X?', record).group().decode("UTF-8")
            identifier = identifier.replace('\x1FaTS268\x1Fb', '') # removes the $a
            
            if args.verbose:
                print(f"Identifier: {identifier}\n")
                
            
            #print("I am here 5")
            def request_data(record):
                #print("I am here 6")
                r = wskey.post(serviceURL + "/manage/bibs", data=record, headers={"Content-Type":"application/marc"})   
                r.raise_for_status
                status = r.raise_for_status
                
                print(f"Status: {status}\n")
                
                result = r.content
                result = result.decode("UTF-8")
                
                return result
                   
            
            with open(output6, 'w') as out6:
                for attempt in range(max_retries): 
                    try:
                        # Send the record to the API Request
                        result = request_data(record)
                        if 'API Key or Authorization header is required' in result or '!DOCTYPE html' in result:
                            out6.write(f"{identifier}.\n {result[:44]}\n")
                            print(f'Error encountered: {result[:44]}')
                            print(f'Retrying request for Identifier: {identifier}\n')
                            fetch_token()
                            result = request_data(record)
                            print(f"Success for Identifier: {identifier}\n")
                            #insert_identifier = f'"IDENTIFIER" : "{identifier}",\n'
                            #modified_result = result[:1] + insert_identifier + result[1:]
                        else:    
                            print(f"Success for Identifier: {identifier}\n")
                            #Insert identifier in the result
                            #insert_identifier = f'"IDENTIFIER" : "{identifier}",\n'
                            #modified_result = result[:1] + insert_identifier + result[1:]                    
                            if args.verbose:
                               print(f"Modified result:\n{modified_result}\n")
                               
                        #print(f"Modified_result = {modified_result}\n Records_count = {records_count}\n, Nr = {nr}\n")
                        #return modified_result, records_count, nr
                        return result, records_count, nr

                    except RetryError:
                        print(f"-->Retry failed after {max_retries} attempts.")
                        out6.write(f"Retry failed after {max_retries} attempts for ID: {identifier}.\n")
                    
                    except requests.exceptions.HTTPError as err:
                        print("HTTP Error:")
                        print(err)
    except BaseException as err:
        print("Base Exception error:")
        print(err)

            
def create_report(output1):

    if not output1:
        raise ValueError("Output1 is empty or None. Please provide valid JSON data.")
    
    """
    else:
        print(f"Type of output1: {type(output1)}")
        print(f"Content of output1: {output1[:100]}")
    """

    #Read the JSON file
    with open(output1, 'r', encoding='UTF-8') as file:
        output1 = file.read()
    
    out3 = []
    out_csv = []
    out2_content = []
    out5_content = []
    
    #Parse the JSON
    try:
        data = json.loads(output1)
        if args.verbose:
            print("JSON structure:")
            print(json.dumps(data, indent=2)) # Print full Json
        if args.verbose:
            print(f"Data loaded: \n {data}")
    except json.JSONDecodeError as e:
        print(f"The variable is not valid JSON. Error: {e}")
        return
    
    # Process each record in the "records" list:
    records = data.get("records",[])
    for record in records:
        identifier = record.get("IDENTIFIER")
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
        record_errors = validation_errors.get( "recordLevelErrors", [])
        fixedfield_errors = validation_errors.get( "fixedFieldErrors", [])
        #print(f"Type of fixedfield_errors: {type(fixedfield_errors)}")
        varfield_errors = validation_errors.get( "variableFieldErrors", [])
    
        # Combine all error types into one list that you can iterate through
        all_errors = record_errors + fixedfield_errors + varfield_errors
           
        if 'The provided Bib is invalid' in description:
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
    formatted_datetime = current_datetime.strftime("%Y-%m-%d.%H.%M.%S")
    
    # Token refresher call
    token_thread = threading.Thread(target=token_refresher, daemon=True)
    token_thread.start()
    
    # Check if an argument was provided
    if len(sys.argv) < 2:
        print('Usage: mdt_misc_validatebib.py "<file_pattern>"\n file_pattern must be enclosed in "')
        sys.exit(1)
        
    # Get the file pattern from the command line
    file_pattern = sys.argv[1]
    function = sys.argv[2]
    
    # Use glob to expand the wildcard pattern
    file_list = glob.glob(file_pattern)
    
    adjs_pattern = f"{file_pattern.replace('*','_')}"
    adjs_f_pattern = re.sub(r'\..{3}', '', adjs_pattern)
    
    
    # Check if files were found
    if not file_list:
        print(f"No files found matching the pattern: {file_pattern}")
        sys.exit(1)
            
    
    if function == 'full' or function == 'add':
        if all(os.path.splitext(file)[1] == '.mrc' for file in file_list):
            print(f"\n***Format file approved '.mrc'\n")
            print("***Identifier will be the first digit string in the mrc record\nHeidelberg will be PPN in 029 (DE-627)")
        else:
            print(f"\n!ERROR: All input files must be '.mrc'\n\nExiting program without execution...\n")
            sys.exit(1)
    
    elif function == 'result':
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
    if len(file_list) == 1:
        outfile_rest=f"{file_pattern[:-4]}.{script_name}.{institution}.{formatted_datetime}"
        output6 = f"{outfile_rest}.LOG.txt"

    else:
        outfile_rest=f"{adjs_f_pattern}.{script_name}.{institution}.{formatted_datetime}"
        output6 = f"{outfile_rest}.LOG.txt"
    
    
    
    
    # Select function to execute based on the second argument 
    if function == 'full':
        # Process each file found 
        outfile=f"{adjs_f_pattern}.{script_name}.{institution}.{formatted_datetime}"
        output1=f"{outfile}.ValidationResponse.json"
        with open(output1, 'a', encoding='utf-8') as out1:
                out1.write(f'{{\n "records" : [\n')
        for file_name in file_list:
            print("\n==================================================")
            print(f"-->Input file processed: {file_name}\n")

            # Obtain the json response from API
            main(file_name)
            
        with open(output1, 'a', encoding='utf-8') as out1:
                out1.write(f'\n]\n}}')     
            
        all_out3, all_out5, all_out2, all_out_csv = create_report(output1)
        
        print(f"All_out3: {all_out3}")
        print(f"all_out5: {all_out5}")
        print(f"all_out2: {all_out2}")
        print(f"all_out_csv: {all_out_csv}")

            
            
    elif function == 'add':        
        # Process each file found 
        outfile=f"{adjs_f_pattern}.{script_name}.{institution}.{formatted_datetime}"
        output1=f"{outfile}.AddResponse.json"
        #with open(output1, 'a', encoding='utf-8') as out1:
                #out1.write(f'{{\n "records" : [\n')
        for file_name in file_list:
            print("\n==================================================")
            print(f"-->Input file processed: {file_name}\n")

            main(file_name)
        #with open(output1, 'a', encoding='utf-8') as out1:
                #out1.write(f'\n]\n}}')    
            

    elif function == 'result':        
        print("\n==================================================")
        print(f"-->Input file processed: {file_pattern}\n")
        
        # Process the json result file
        all_out3, all_out5, all_out2, all_out_csv =  create_report(file_pattern)
        
       

    """Reports and Stats"""
    if function == 'full' or function == 'result':
        # Create report 
        output2=f"{outfile_rest}.ALL.ValidationReport.txt"
        with open(output2, 'w', encoding='UTF-8') as out2:
            out2.writelines(all_out2)
        
        output4=f"{outfile_rest}.ALL.ValidationStats.txt"
        output7=f"{outfile_rest}.ALL.ValidationCSV.csv"
        
        # Send the values to the STATS function
        stats(all_out3, all_out_csv)

    
    
    print(f"\n***End of file***")
    
    """Valid Bibs"""
    if function == 'full' or function == 'result':
        # Write valid bibs only if found

        output5=f"{outfile_rest}.ALL.ValidBibs.txt"
        if all_out5:
            with open(output5, 'w', encoding='UTF-8') as out5:
                out5.writelines(all_out5)
        else:
            print("\nNo valid BIBs returned\n")
        

    print(f"***End of script***")

