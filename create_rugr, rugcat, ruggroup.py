"""
Create SAS Formats for Resource Utilization Group, Version IV (RUG-IV)

Analyst: Will D. Leone
Last Updated: February 21, 2019

Purpose:
  - Retrieve the latest CMS crosswalk for RUG categories/groups.
  - Clean and export this data to SAS format tables on the SAS server.
  - Create a CSV copy of the crosswalk to accompany these datasets.

 Prerequisites:
  - In addition to installing Python/Anaconda on your computer,
    you will also need to install the requests and saspy modules using the
    'conda install requests' and 'conda install saspy' commands in Anaconda
    Prompt.
  - You will also need to configure saspy using the instructions given here:
      https://confluence.evolenthealth.com/display/AnalyticsActuarial
      /SASpy+configuration+tutorial

 Next Steps:
 The output SAS datasets and CSV file will be saved to the SOURCE file within
 the all-client format directory (/sasprod/dw/formats). Once QA is done, the
 old datasets will need to be saved to the OLD subdirectory and the new datasets
 will need to be pushed into the main directory.

"""

import requests
import json
import csv
import os.path
import re
import saspy
import pandas as pd
import sys
from copy import deepcopy
from operator import itemgetter

# Obtain the Socrata API site using the API button the dataset's landing page
site = ('https://data.cms.gov/resource/qmvt-uw4p.json?'
        '$select=rug, rug_description')    #SoQL: Socrata Query Language API
ecode = 'utf-8'

# OUTPUT DATA PARAMETERS
sas = saspy.SASsession(cfgname='pdw_config')
sas_code = sas.submit("""
    LIBNAME fmt "/sasprod/dw/formats/source/staging";
    """)
grid = ("//grid/sasprod/dw/formats/source")
out_file = ("//grid/sasprod/dw/formats/source/references/"
            "cms_rugcat_ruggroup.xlsx")

# Pull DATA.CMS.GOV JSON data into an in-memory Pandas DataFrame
outdf = ''    # initialize output DataFrame
with requests.Session() as my_session:
    raw_source = my_session.get(site)    # site data as a Requests object
    outdf = pd.read_json(raw_source.text)   # convert site data into dataframe

outdf.insert(2, 'RUG_Group', '')
outdf.columns=['RUG', 'RUG_Category', 'RUG_Group']
for index in range(len(outdf)):    # clean data, extract RUG category
    cat = outdf.iat[index, 1]
    if cat.find(' -') != -1:
        outdf.iat[index, 1] = cat[: cat.find(' -')].strip()
            # iat[] is faster than iloc[] for single-value pulls
    else:
        outdf.iat[index, 1] = cat.strip()
    if 'Medium' in cat[:6]:
        outdf.iat[index, 1] = outdf.iat[index, 1].replace('Medium ', '')
        outdf.iat[index, 2] = 'Medium'
    elif 'High' in cat[:4]:
        outdf.iat[index, 1] = outdf.iat[index, 1].replace('High ', '')
        outdf.iat[index, 2] = 'High'
    elif 'Very-High' in cat:
        outdf.iat[index, 1] = outdf.iat[index, 1].replace('Very-High ', '')
        outdf.iat[index, 2] = 'Very-High'
    elif 'Ultra-High' in cat:
        outdf.iat[index, 1] = outdf.iat[index, 1].replace('Ultra-High ', '')
        outdf.iat[index, 2] = 'Ultra-High'
    else:
        outdf.iat[index, 2] = 'Other'

# Send the finalized RUG DataFrame to an Excel reference file
outdf.to_excel(out_file, sheet_name='rugcat and ruggroup', engine='xlsxwriter')

# Prepare and export the finalized SAS formats
outdf.insert(3, 'fmtname', '')
outdf.insert(4, 'type', '')
out_rugcat = pd.concat([
        outdf.RUG
        , outdf.RUG_Category
        , outdf.fmtname
        , outdf.type
    ], axis=1)
out_ruggroup = pd.concat([
        outdf.RUG
        , outdf.RUG_Group
        , outdf.fmtname
        , outdf.type
    ], axis=1)
    # If dups were an issue, could use drop_duplicates method on DataFrames
out_rugcat.columns = ['start', 'label', 'fmtname', 'type']
out_ruggroup.columns = ['start', 'label', 'fmtname', 'type']
# Now set the default format variables
for index in range(len(outdf)):  # exploit equal length of all dataframes
    out_rugcat.iat[index, 2] = 'rugcat'
    out_ruggroup.iat[index, 2] = 'ruggroup'
    out_rugcat.iat[index, 3] = 'C'
    out_ruggroup.iat[index, 3] = 'C'
# Export the finalized DataFrames
sas_out = sas.df2sd(out_rugcat, table='rugcat', libref='fmt')
sas_out = sas.df2sd(out_ruggroup, table='ruggroup', libref='fmt')
sas.disconnect()
