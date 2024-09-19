import requests
import streamlit as st
import pandas as pd
import re
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import chardet
import os
import concurrent.futures
import io
import json
from placekey.api import PlacekeyAPI
from io import StringIO

placekey_api_key = "kPiQScbIp1BlxMChirEljha7fh2FatF8"
url = "https://placekey.nyc3.cdn.digitaloceanspaces.com/placekeys_standardized%20copy%207.csv"
zrl = "https://placekey.nyc3.cdn.digitaloceanspaces.com/REI%20Sift-All%20data-08262024_standardized+placekeys%20(4).csv"

# Make a request to get the first CSV file
response = requests.get(url)
if response.status_code == 200:
    # Read the CSV content using pandas
    cache_file_path = StringIO(response.text)
else:
    raise Exception(f"Failed to retrieve the file. Status code: {response.status_code}")

# Make a request to get the second CSV file
req = requests.get(zrl)
if req.status_code == 200:
    # Read the CSV content using pandas
    REI_local_path = StringIO(req.text)
else:
    raise Exception(f"Failed to retrieve the file. Status code: {req.status_code}")

# Debugging: Check if REI_local_path is correctly assigned
if 'REI_local_path' not in locals():
    raise Exception("REI_local_path is not defined. Something went wrong with the CSV retrieval.")

# Load the REI data into a DataFrame
rei = pd.read_csv(REI_local_path, dtype={
    'street_address': str,
    'placekey': str,
    'city': str,
    'region': str,
    'postal_code': str,
    'Parcel number': str
})

# Initialize Placekey API
pk_api = PlacekeyAPI(placekey_api_key)

# Load the cache file and drop duplicates
cache_df = pd.read_csv(cache_file_path, dtype={
    'street_address': str,
    'placekey': str,
    'city': str,
    'region': str,
    'postal_code': str,
    'Parcel number': str
})
def clean_api_responses(data_jsoned, responses):
    print("Number of original records: ", len(data_jsoned))
    print('Total individual queries returned: ', len(responses))

    # Filter out invalid responses
    responses_cleaned = [resp for resp in responses if 'query_id' in resp]
    print('Total successful query responses: ', len(responses_cleaned))

    # Log any missing records for debugging
    missing_records = len(data_jsoned) - len(responses_cleaned)
    if missing_records > 0:
        print(f"Warning: {missing_records} records were not returned by the API.")

    return responses_cleaned

def check_cache(data_jsoned, cache_df):
    # Create a dictionary with (street_address, city) as the key and placekey as the value
    cache_dict = {(row['street_address'], row['city']): row['placekey'] for _, row in cache_df.iterrows()}
    cached_responses = []
    api_requests = []

    for record in data_jsoned:
        address_city_key = (record['street_address'], record['city'])
        if address_city_key in cache_dict:
            cached_responses.append({
                'query_id': record['query_id'],
                'placekey': cache_dict[address_city_key]
            })
        else:
            api_requests.append(record)

    st.write(f"Number of records found in cache: {len(cached_responses)}")
    st.write(f"Number of records needing API lookup: {len(api_requests)}")

    return cached_responses, api_requests

# Phase 1: Handle records with Parcel numbers
def update_records_from_cache(df, rei):
    print("Updating records from cache")
    records_found = []

    for idx, row in df.iterrows():
        parcel_number = row['Parcel number']
        if not pd.isna(parcel_number) and parcel_number in rei['Parcel number'].values:
            cache_record = rei[rei['Parcel number'] == parcel_number].iloc[0]
            df.at[idx, 'street_address'] = cache_record['street_address']
            df.at[idx, 'city'] = cache_record['city']
            df.at[idx, 'region'] = cache_record['region']
            df.at[idx, 'postal_code'] = cache_record['postal_code']
            records_found.append(row)

    if records_found:
        st.write("Records found and updated by Parcel number:")
        st.write(pd.DataFrame(records_found))

    return df

def update_records_with_placekeys(df_join_placekey, rei):
    print("Updating with Placekeys from REI")
    # Ensure 'placekey' in 'rei' is unique by dropping duplicates
    rei_unique = rei.drop_duplicates(subset='placekey')

    # Create a mask to find matching placekeys in both DataFrames
    mask = df_join_placekey['placekey'].isin(rei_unique['placekey'])

    # Create a mapping DataFrame with relevant columns from 'rei'
    rei_mapping = rei_unique.set_index('placekey')[['street_address', 'city', 'region', 'postal_code']]

    # Update the corresponding columns in df_join_placekey with the values from 'rei'
    df_join_placekey.loc[mask, ['street_address', 'city', 'region', 'postal_code']] = (
        df_join_placekey.loc[mask, 'placekey'].map(rei_mapping.to_dict(orient='index')).apply(pd.Series)
    )
    print("Done updating")
    return df_join_placekey

def filter_franklin_county(df, rei):
    franklin_county_cities = [
        "Bexley", "Blacklick Estates", "Brice", "Canal Winchester", "Columbus", "Dublin",
        "Gahanna", "Grandview Heights", "Grove City", "Groveport", "Galloway", "Grandview", "Hilliard", "New Albany",
        "Obetz", "Reynoldsburg", "Upper Arlington", "Westerville", "Whitehall", "Worthington",
        "Lincoln Village", "Minerva Park", "Marble Cliff", "Urbancrest", "Valleyview", "Blacklick", "Darbydale",
        "Etna", "Harrisburg", "Lockbourne", "London", "Marble Cliff", "New Albany", "Orient", "Pataskala", "Plain City",
        "Powell", "Reynoldsburg", "Upper Arlington", "Urbancrest","Colombus Grove"
    ]

    # Identify new records in Franklin County and remove them
    new_records_in_franklin = df[
        (df['city'].isin(franklin_county_cities)) & (~df['placekey'].isin(rei['placekey']))]
    if not new_records_in_franklin.empty:
        print("Removing new records in Franklin County:")
        print(len(new_records_in_franklin))
        print(new_records_in_franklin)

    df_filtered = df[~((df['city'].isin(franklin_county_cities)) & (~df['placekey'].isin(rei['placekey'])))]
    return df_filtered

def parallel_process(df, func, column_name, new_column_name):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        df[new_column_name] = list(executor.map(func, df[column_name]))

# Compile the regex patterns beforehand
address_patterns = [(re.compile(pattern, re.IGNORECASE), replacement) for pattern, replacement in {
    r'\bavenue\b\.?': 'Ave', r'\bav\b\.?': 'Ave', r'\bave\b\.?': 'Ave',
    r'\bstreet\b\.?': 'St', r'\bstr\b\.?': 'St', r'\bst\b\.?': 'St',
    r'\bboulevard\b\.?': 'Blvd', r'\bblv\b\.?': 'Blvd', r'\bblvd\b\.?': 'Blvd',
    r'\banex\b\.?': 'Anx', r'\bannex\b\.?': 'Anx', r'\bannx\b\.?': 'Anx',
    r'\balley\b\.?': 'Aly', r'\ballee\b\.?': 'Aly', r'\bally\b\.?': 'Aly',
    r'\bcamp\b\.?': 'Cp', r'\bcmp\b\.?': 'Cp',
    r'\bcanyn\b\.?': 'Cyn', r'\bcanyon\b\.?': 'Cyn', r'\bcnyn\b\.?': 'Cyn',
    r'\bcen\b\.?': 'Ctr', r'\bcent\b\.?': 'Ctr', r'\bcenter\b\.?': 'Ctr',
    r'\bcentr\b\.?': 'Ctr', r'\bcentre\b\.?': 'Ctr', r'\bcnter\b\.?': 'Ctr',
    r'\bclif\b\.?': 'Clf', r'\bclf\b\.?': 'Clf',
    r'\bclifs\b\.?': 'Clfs', r'\bclfs\b\.?': 'Clfs',
    r'\bclub\b\.?': 'Clb', r'\bclb\b\.?': 'Clb',
    r'\bcommon\b\.?': 'Cmn',
    r'\bcr\b\.?': 'Cir',
    r'\btrafficway\b\.?': 'Trfy',
    r'\bcommons\b\.?': 'Cmns',
    r'\btrack\b\.?': 'Trak', r'\btracks\b\.?': 'Trak', r'\btrak\b\.?': 'Trak', r'\btrk\b\.?': 'Trak',
    r'\btrks\b\.?': 'Trak',
    r'\bmill\b\.?': 'Ml',
    r'\bmills\b\.?': 'Mls',
    r'\bvill\b\.?': 'Vlg', r'\bvillag\b\.?': 'Vlg', r'\bvillage\b\.?': 'Vlg', r'\bvillg\b\.?': 'Vlg',
    r'\bvilliage\b\.?': 'Vlg', r'\bvlg\b\.?': 'Vlg',
    r'\bvills\b\.?': 'Vlgs', r'\bvillags\b\.?': 'Vlgs', r'\bvillages\b\.?': 'Vlgs',
    r'\bville\b\.?': 'Vl', r'\bvl\b\.?': 'Vl',
    r'\bwell\b\.?': 'Wl', r'\bwells\b\.?': 'Wls',
    r'\bhllw\b\.?': 'Holw', r'\bhollow\b\.?': 'Holw', r'\bhollows\b\.?': 'Holw', r'\bholw\b\.?': 'Holw',
    r'\bholws\b\.?': 'Holw',
    r'\bis\b\.?': 'IS', r'\bisland\b\.?': 'IS', r'\bislnd\b\.?': 'IS',
    r'\biss\b\.?': 'ISS', r'\bislands\b\.?': 'ISS', r'\bislnds\b\.?': 'ISS',
    r'\bisle\b\.?': 'Isle', r'\bisles\b\.?': 'Isle',
    r'\bmissn\b\.?': 'Msn', r'\bmssn\b\.?': 'Msn',
    r'\bmotorway\b\.?': 'Mtwy',
    r'\bthroughway\b\.?': 'Trwy',
    r'\bcorner\b\.?': 'Cor', r'\bcor\b\.?': 'Cor',
    r'\bcorners\b\.?': 'Cors', r'\bcors\b\.?': 'Cors',
    r'\bcourse\b\.?': 'Crse', r'\bcrse\b\.?': 'Crse',
    r'\bdam\b\.?': 'Dm', r'\bdm\b\.?': 'Dm',
    r'\bestate\b\.?': 'Est', r'\best\b\.?': 'Est',
    r'\bestates\b\.?': 'Ests', r'\bests\b\.?': 'Ests',
    r'\bfall\b\.?': 'Fall',
    r'\bfalls\b\.?': 'Fls', r'\bfls\b\.?': 'Fls',
    r'\bfreeway\b\.?': 'Fwy', r'\bfreewy\b\.?': 'Fwy', r'\bfrway\b\.?': 'Fwy', r'\bfwy\b\.?': 'Fwy',
    r'\bht\b\.?': 'Hts', r'\bhts\b\.?': 'Hts',
    r'\bmnrs\b\.?': 'Mnrs', r'\bmanors\b\.?': 'Mnrs',
    r'\bgrove\b\.?': 'Grv', r'\bgrov\b\.?': 'Grv',
    r'\bgroves\b\.?': 'Grvs', r'\bgrovs\b\.?': 'Grvs',
    r'\bharb\b\.?': 'Hbr', r'\bharbor\b\.?': 'Hbr', r'\bharbr\b\.?': 'Hbr', r'\bhbr\b\.?': 'Hbr',
    r'\bharbors\b\.?': 'Hbrs',
    r'\bmdw\b\.?': 'Mdws', r'\bmdws\b\.?': 'Mdws', r'\bmeadows\b\.?': 'Mdws', r'\bmedows\b\.?': 'Mdws',
    r'\bhill\b\.?': 'Hl', r'\bhl\b\.?': 'Hl',
    r'\bhills\b\.?': 'Hls', r'\bhls\b\.?': 'Hls',
    r'\bmnr\b\.?': 'Mnr', r'\bmanor\b\.?': 'Mnr',
    r'\bflat\b\.?': 'Flt', r'\bflt\b\.?': 'Flt',
    r'\bflats\b\.?': 'Flts', r'\bflts\b\.?': 'Flts',
    r'\bglen\b\.?': 'Gln', r'\bgln\b\.?': 'Gln',
    r'\bglens\b\.?': 'Glns', r'\bglns\b\.?': 'Glns',
    r'\bferry\b\.?': 'Fry', r'\bfrry\b\.?': 'Fry',
    r'\bfry\b\.?': 'Fry',
    r'\bgreen\b\.?': 'Grn', r'\bgrn\b\.?': 'Grn',
    r'\bgreens\b\.?': 'Grns', r'\bgrns\b\.?': 'Grns',
    r'\blck\b\.?': 'Lck', r'\block\b\.?': 'Lck',
    r'\bfield\b\.?': 'Fld', r'\bfld\b\.?': 'Fld',
    r'\blcks\b\.?': 'Lcks', r'\blocks\b\.?': 'Lcks',
    r'\bfields\b\.?': 'Flds', r'\bflds\b\.?': 'Flds',
    r'\bloaf\b\.?': 'Lf', r'\blf\b\.?': 'Lf',
    r'\bcenters\b\.?': 'Ctrs',
    r'\barcade\b\.?': 'arc',
    r'\bbayou\b\.?': 'Byu', r'\bbayoo\b\.?': 'Byu',
    r'\bbeach\b\.?': 'Bch',
    r'\bbend\b\.?': 'Bnd',
    r'\bhaven\b\.?': 'Hvn', r'\bhvn\b\.?': 'Hvn',
    r'\bldg\b\.?': 'Ldg', r'\bldge\b\.?': 'Ldg', r'\blodg\b\.?': 'Ldg', r'\blodge\b\.?': 'Ldg',
    r'\bbluf\b\.?': 'Blf', r'\bbluff\b\.?': 'Blf', r'\bblf\b\.?': 'Blf',
    r'\bbluffs\b\.?': 'Blfs',
    r'\bbot\b\.?': 'Btm', r'\bbottm\b\.?': 'Btm', r'\bbottom\b\.?': 'Btm',
    r'\bbr\b\.?': 'Br', r'\bbrnch\b\.?': 'Br', r'\bbranch\b\.?': 'Br',
    r'\bbrdge\b\.?': 'Brg', r'\bbridge\b\.?': 'Blvd',
    r'\bbrook\b\.?': 'Brk',
    r'\bbrooks\b\.?': 'Brks',
    r'\bburg\b\.?': 'Bg',
    r'\bburgs\b\.?': 'Bgs',
    r'\bunion\b\.?': 'Un', r'\bunions\b\.?': 'Uns',
    r'\bbypa\b\.?': 'Byp', r'\bbypas\b\.?': 'Byp', r'\bbyps\b\.?': 'Byp',
    r'\broute\b\.?': 'Rte', r'\brt\b\.?': 'Rte',
    r'\bbl\b\.?': 'Blvd', r'\broad\b\.?': 'Rd', r'\brd\b\.?': 'Rd',
    r'\bcourt\b\.?': 'Ct', r'\bct\b\.?': 'Ct',
    r'\bcourts\b\.?': 'Cts', r'\bcts\b\.?': 'Cts',
    r'\bcove\b\.?': 'Cv', r'\bcv\b\.?': 'Cv',
    r'\bcoves\b\.?': 'Cvs', r'\bcvs\b\.?': 'Cvs',
    r'\bcrescent\b\.?': 'Cres', r'\bcres\b\.?': 'Cres', r'\bcrsent\b\.?': 'Cres', r'\bcrsnt\b\.?': 'Cres',
    r'\blgts\b\.?': 'Lgts', r'\blights\b\.?': 'Lgts',
    r'\bcrest\b\.?': 'Crst',
    r'\brest\b\.?': 'Rst', r'\brst\b\.?': 'Rst',
    r'\briv\b\.?': 'Riv', r'\briver\b\.?': 'Riv', r'\brvr\b\.?': 'Riv', r'\brivr\b\.?': 'Riv',
    r'\bcrossroad\b\.?': 'Xrd',
    r'\bcrossroads\b\.?': 'Xrds',
    r'\bcurve\b\.?': 'Curv',
    r'\bdale\b\.?': 'Dl', r'\bdl\b\.?': 'Dl',
    r'\bdiv\b\.?': 'Dv', r'\bdivide\b\.?': 'Dv', r'\bdvd\b\.?': 'Dv', r'\bdv\b\.?': 'Dv',
    r'\blgt\b\.?': 'Lgt', r'\blight\b\.?': 'Lgt',
    r'\bforest\b\.?': 'Frst', r'\bforests\b\.?': 'Frst', r'\bfrst\b\.?': 'Frst',
    r'\bforges\b\.?': 'Frgs',
    r'\bforge\b\.?': 'Frg', r'\bfrg\b\.?': 'Frg', r'\bforg\b\.?': 'Frg',
    r'\blakes\b\.?': 'Lks', r'\blks\b\.?': 'Lks',
    r'\blake\b\.?': 'Lk', r'\blk\b\.?': 'Lk',
    r'\blanding\b\.?': 'Lang', r'\blndg\b\.?': 'Lang', r'\blndng\b\.?': 'Lang',
    r'\bknls\b\.?': 'Knls', r'\bknols\b\.?': 'knls',
    r'\bdrive\b\.?': 'Dr', r'\bdr\b\.?': 'Dr',
    r'\blane\b\.?': 'Ln', r'\bln\b\.?': 'Ln',
    r'\bterrace\b\.?': 'Ter', r'\bter\b\.?': 'Ter',
    r'\bplace\b\.?': 'Pl', r'\bpl\b\.?': 'Pl',
    r'\bcircle\b\.?': 'Cir', r'\bcir\b\.?': 'Cir', r'\bcirc\b\.?': 'Cir', r'\bcircl\b\.?': 'Cir',
    r'\bcrcle\b\.?': 'Cir',
    r'\bsquare\b\.?': 'Sq', r'\bsq\b\.?': 'Sq',
    r'\bhighway\b\.?': 'Hwy', r'\bhwy\b\.?': 'Hwy',
    r'\bplaza\b\.?': 'Plz', r'\bplz\b\.?': 'Plz',
    r'\borch\b\.?': 'Orch', r'\borchard\b\.?': 'Orch', r'\borchrd\b\.?': 'Orch',
    r'\bmnt\b\.?': 'Mt', r'\bmt\b\.?': 'Mt', r'\bmount\b\.?': 'Mt',
    r'\bnck\b\.?': 'Nck', r'\bneck\b\.?': 'Nck',
    r'\bjunction\b\.?': 'Jct', r'\bjct\b\.?': 'Jct', r'\bjction\b\.?': 'Jct', r'\bjctn\b\.?': 'Jct',
    r'\bjuncton\b\.?': 'Jct', r'\bjunctn\b\.?': 'Jct',
    r'\bjunctions\b\.?': 'Jcts', r'\bjcts\b\.?': 'Jcts',
    r'\bknl\b\.?': 'Knl', r'\bknol\b\.?': 'knl',
    r'\bmountain\b\.?': 'Mtn', r'\bmtn\b\.?': 'Mtn', r'\bmntain\b\.?': 'Mtn', r'\bmntn\b\.?': 'Mtn',
    r'\bmountin\b\.?': 'Mtn', r'\bmtin\b\.?': 'Mtn',
    r'\bexpressway\b\.?': 'Expy', r'\bexpy\b\.?': 'Expy',
    r'\bextension\b\.?': 'Ext', r'\bext\b\.?': 'Ext',
    r'\bextensions\b\.?': 'Exts', r'\bexts\b\.?': 'Exts',
    r'\bford\b\.?': 'Frd', r'\bfrd\b\.?': 'Frd',
    r'\bfords\b\.?': 'Frds', r'\bfrds\b\.?': 'Frds',
    r'\bfork\b\.?': 'Frk', r'\bfrk\b\.?': 'Frk',
    r'\bforks\b\.?': 'Frks', r'\bfrks\b\.?': 'Frks',
    r'\bgarden\b\.?': 'Gdn', r'\bgardn\b\.?': 'Gdn', r'\bgrden\b\.?': 'Gdn', r'\bgrdn\b\.?': 'Gdn',
    r'\bgardens\b\.?': 'Gdns', r'\bgardns\b\.?': 'Gdns', r'\bgrdens\b\.?': 'Gdns', r'\bgrdns\b\.?': 'Gdns',
    r'\bkey\b\.?': 'Ky', r'\bky\b\.?': 'Ky',
    r'\bkeys\b\.?': 'Kys', r'\bkys\b\.?': 'Kys',
    r'\btrail\b\.?': 'Trl', r'\btrl\b\.?': 'Trl',
    r'\btrailer\b\.?': 'Trlr', r'\btrlr\b\.?': 'Trlr', r'\btrlrs\b\.?': 'Trlr',
    r'\btrnpk\b\.?': 'Tpke', r'\bturnpike\b\.?': 'Tpke', r'\bturnpk\b\.?': 'Tpke',
    r'\btunel\b\.?': 'Tunl', r'\btunl\b\.?': 'Tunl', r'\btunls\b\.?': 'Tunl', r'\btunnul\b\.?': 'Tunl',
    r'\bgateway\b\.?': 'Gtwy', r'\bgtwy\b\.?': 'Gtwy',
    r'\bcrossing\b\.?': 'Xing', r'\bxing\b\.?': 'Xing', r'\bcrssng\b\.?': 'Xing',
    r'\bfort\b\.?': 'Ft', r'\bft\b\.?': 'Ft',
    r'\bvalley\b\.?': 'Vly', r'\bvally\b\.?': 'Vly', r'\bvlly\b\.?': 'Vly',
    r'\bvalleys\b\.?': 'Vlys', r'\bvallys\b\.?': 'Vlys', r'\bvllys\b\.?': 'Vlys',
    r'\bview\b\.?': 'Vw', r'\bvw\b\.?': 'Vw', r'\bviews\b\.?': 'Vws', r'\bvws\b\.?': 'Vws',
    r'\brun\b\.?': 'Run',
    r'\bcreek\b\.?': 'Crk', r'\bcrk\b\.?': 'Crk',
    r'\boval\b\.?': 'Oval', r'\bovl\b\.?': 'Oval',
    r'\boverpass\b\.?': 'Opas',
    r'\btrace\b\.?': 'Trce', r'\btraces\b\.?': 'Trce',
    r'\bpark\b\.?': 'Park', r'\bprk\b\.?': 'Park',
    r'\bparks\b\.?': 'Parks',
    r'\bpassage\b\.?': 'Psge',
    r'\bpath\b\.?': 'Path',
    r'\bparkway\b\.?': 'Pkwy', r'\bparkwy\b\.?': 'Pkwy', r'\bpkway\b\.?': 'Pkwy', r'\bpkwy\b\.?': 'Pkwy',
    r'\bway\b\.?': 'Way', r'\bwy\b\.?': 'Way',
    r'\bplain\b\.?': 'Pln',
    r'\bpoint\b\.?': 'Pt', r'\bpoints\b\.?': 'Pts',
    r'\bport\b\.?': 'Prt', r'\bports\b\.?': 'Prts',
    r'\bprairie\b\.?': 'Pr', r'\bprr\b\.?': 'Pr',
    r'\brapid\b\.?': 'Rpd', r'\brpd\b\.?': 'Rpd',
    r'\brapids\b\.?': 'Rpds', r'\brpds\b\.?': 'Rpds',
    r'\bsta\b\.?': 'Sta', r'\bstation\b\.?': 'Sta', r'\bstatn\b\.?': 'Sta', r'\bstn\b\.?': 'Sta',
    r'\bstra\b\.?': 'Stra', r'\bstrav\b\.?': 'Stra', r'\bstraven\b\.?': 'Stra', r'\bstravenue\b\.?': 'Stra',
    r'\bstravn\b\.?': 'Stra', r'\bstrvn\b\.?': 'Stra',
    r'\bstream\b\.?': 'Strm', r'\bstreme\b\.?': 'Strm',
    r'\bsmt\b\.?': 'Smt', r'\bsumit\b\.?': 'Smt', r'\bsumitt\b\.?': 'Smt', r'\bsummit\b\.?': 'Smt',
    r'\brad\b\.?': 'Radl', r'\bradial\b\.?': 'Radl', r'\bradiel\b\.?': 'Radl', r'\bradl\b\.?': 'Radl',
    r'\bshl\b\.?': 'Shl', r'\bshoal\b\.?': 'Shl', r'\bshls\b\.?': 'Shls', r'\bshoals\b\.?': 'Shls',
    r'\bshr\b\.?': 'Shr', r'\bshoar\b\.?': 'Shr', r'\bshore\b\.?': 'Shr', r'\bshrs\b\.?': 'Shrs',
    r'\bshoars\b\.?': 'Shrs', r'\bshores\b\.?': 'Shrs',
    r'\bspg\b\.?': 'Spg', r'\bspng\b\.?': 'Spg', r'\bspring\b\.?': 'Spg', r'\bsprng\b\.?': 'Spg',
    r'\bspgs\b\.?': 'Spgs', r'\bspngs\b\.?': 'Spgs', r'\bsprings\b\.?': 'Spgs', r'\bsprngs\b\.?': 'Spgs',
}.items()]

directional_patterns = {re.compile(pattern, re.IGNORECASE): replacement for pattern, replacement in {
    r'\bnorth\b\.?': 'N', r'\bnorthern\b\.?': 'N', r'\bn\b\.?': 'N',
    r'\bwest\b\.?': 'W', r'\bwestern\b\.?': 'W', r'\bw\b\.?': 'W',
    r'\beast\b\.?': 'E', r'\beastern\b\.?': 'E', r'\be\b\.?': 'E',
    r'\bsouth\b\.?': 'S', r'\bsouthern\b\.?': 'S', r'\bs\b\.?': 'S'
}.items()}

ordinal_mapping = {
    'first': '1st', 'second': '2nd', 'third': '3rd', 'fourth': '4th', 'fifth': '5th',
    'sixth': '6th', 'seventh': '7th', 'eighth': '8th', 'ninth': '9th', 'tenth': '10th',
    'eleventh': '11th', 'twelfth': '12th', 'thirteenth': '13th', 'fourteenth': '14th',
    'fifteenth': '15th', 'sixteenth': '16th', 'seventeenth': '17th', 'eighteenth': '18th',
    'nineteenth': '19th', 'twentieth': '20th', 'twenty-first': '21st', 'twenty first': '21st', 'twentyfirst': '21st',
    'twenty-second': '22nd', 'twenty second': '22nd', 'twentysecond': '22nd', 'twenty-third': '23rd',
    'twenty third': '23rd', 'twentythird': '23rd',
    'twenty-fourth': '24th', 'twenty fourth': '24th', 'twentyfourth': '24th', 'twenty-fifth': '25th',
    'twenty fifth': '25th', 'twentyfifth': '25th',
    'twenty-sixth': '26th', 'twenty sixth': '26th', 'twentysixth': '26th', 'twenty-seventh': '27th',
    'twenty seventh': '27th', 'twentyseventh': '27th',
    'twenty-eighth': '28th', 'twenty eighth': '28th', 'twentyeighth': '28th', 'twenty-ninth': '29th',
    'twenty ninth': '29th', 'twentyninth': '29th',
    'thirtieth': '30th', 'thirty-first': '31st', 'thirty first': '31st', 'thirtyfirst': '31st', 'thirty-second': '32nd',
    'thirty second': '32nd', 'thirtysecond': '32nd',
    'thirty-third': '33rd', 'thirty third': '33rd', 'thirtythird': '33rd', 'thirty-fourth': '34th',
    'thirty fourth': '34th', 'thirtyfourth': '34th',
    'thirty-fifth': '35th', 'thirty fifth': '35th', 'thirtyfifth': '35th', 'thirty-sixth': '36th',
    'thirty sixth': '36th', 'thirtysixth': '36th',
    'thirty-seventh': '37th', 'thirty seventh': '37th', 'thirtyseventh': '37th', 'thirty-eighth': '38th',
    'thirty eighth': '38th', 'thirtyeighth': '38th',
    'thirty-ninth': '39th', 'thirty ninth': '39th', 'thirtyninth': '39th', 'fortieth': '40th', 'forty-first': '41st',
    'forty first': '41st', 'fortyfirst': '41st',
    'forty-second': '42nd', 'forty second': '42nd', 'fortysecond': '42nd', 'forty-third': '43rd', 'forty third': '43rd',
    'fortythird': '43rd',
    'forty-fourth': '44th', 'forty fourth': '44th', 'fortyfourth': '44th', 'forty-fifth': '45th', 'forty fifth': '45th',
    'fortyfifth': '45th',
    'forty-sixth': '46th', 'forty sixth': '46th', 'fortysixth': '46th', 'forty-seventh': '47th',
    'forty seventh': '47th', 'fortyseventh': '47th',
    'forty-eighth': '48th', 'forty eighth': '48th', 'fortyeighth': '48th', 'forty-ninth': '49th', 'forty ninth': '49th',
    'fortyninth': '49th',
    'fiftieth': '50th', 'fifty-first': '51st', 'fifty first': '51st', 'fiftyfirst': '51st', 'fifty-second': '52nd',
    'fifty second': '52nd', 'fiftysecond': '52nd',
    'fifty-third': '53rd', 'fifty third': '53rd', 'fiftythird': '53rd', 'fifty-fourth': '54th', 'fifty fourth': '54th',
    'fiftyfourth': '54th',
    'fifty-fifth': '55th', 'fifty fifth': '55th', 'fiftyfifth': '55th', 'fifty-sixth': '56th', 'fifty sixth': '56th',
    'fiftysixth': '56th',
    'fifty-seventh': '57th', 'fifty seventh': '57th', 'fiftyseventh': '57th', 'fifty-eighth': '58th',
    'fifty eighth': '58th', 'fiftyeighth': '58th',
    'fifty-ninth': '59th', 'fifty ninth': '59th', 'fiftyninth': '59th', 'sixtieth': '60th', 'sixty-first': '61st',
    'sixty first': '61st', 'sixtyfirst': '61st',
    'sixty-second': '62nd', 'sixty second': '62nd', 'sixtysecond': '62nd', 'sixty-third': '63rd', 'sixty third': '63rd',
    'sixtythird': '63rd',
    'sixty-fourth': '64th', 'sixty fourth': '64th', 'sixtyfourth': '64th', 'sixty-fifth': '65th', 'sixty fifth': '65th',
    'sixtyfifth': '65th',
    'sixty-sixth': '66th', 'sixty sixth': '66th', 'sixtysixth': '66th', 'sixty-seventh': '67th',
    'sixty seventh': '67th', 'sixtyseventh': '67th',
    'sixty-eighth': '68th', 'sixty eighth': '68th', 'sixtyeighth': '68th', 'sixty-ninth': '69th', 'sixty ninth': '69th',
    'sixtyninth': '69th',
    'seventieth': '70th', 'seventy-first': '71st', 'seventy first': '71st', 'seventyfirst': '71st',
    'seventy-second': '72nd', 'seventy second': '72nd', 'seventysecond': '72nd',
    'seventy-third': '73rd', 'seventy third': '73rd', 'seventythird': '73rd', 'seventy-fourth': '74th',
    'seventy fourth': '74th', 'seventyfourth': '74th',
    'seventy-fifth': '75th', 'seventy fifth': '75th', 'seventyfifth': '75th', 'seventy-sixth': '76th',
    'seventy sixth': '76th', 'seventysixth': '76th',
    'seventy-seventh': '77th', 'seventy seventh': '77th', 'seventyseventh': '77th', 'seventy-eighth': '78th',
    'seventy eighth': '78th', 'seventyeighth': '78th',
    'seventy-ninth': '79th', 'seventy ninth': '79th', 'seventyninth': '79th', 'eightieth': '80th',
    'eighty-first': '81st', 'eighty first': '81st', 'eightyfirst': '81st',
    'eighty-second': '82nd', 'eighty second': '82nd', 'eightysecond': '82nd', 'eighty-third': '83rd',
    'eighty third': '83rd', 'eightythird': '83rd',
    'eighty-fourth': '84th', 'eighty fourth': '84th', 'eightyfourth': '84th', 'eighty-fifth': '85th',
    'eighty fifth': '85th', 'eightyfifth': '85th',
    'eighty-sixth': '86th', 'eighty sixth': '86th', 'eightysixth': '86th', 'eighty-seventh': '87th',
    'eighty seventh': '87th', 'eightyseventh': '87th',
    'eighty-eighth': '88th', 'eighty eighth': '88th', 'eightyeighth': '88th', 'eighty-ninth': '89th',
    'eighty ninth': '89th', 'eightyninth': '89th',
    'ninetieth': '90th', 'ninety-first': '91st', 'ninety first': '91st', 'ninetyfirst': '91st', 'ninety-second': '92nd',
    'ninety second': '92nd', 'ninetysecond': '92nd',
    'ninety-third': '93rd', 'ninety third': '93rd', 'ninetythird': '93rd', 'ninety-fourth': '94th',
    'ninety fourth': '94th', 'ninetyfourth': '94th',
    'ninety-fifth': '95th', 'ninety fifth': '95th', 'ninetyfifth': '95th', 'ninety-sixth': '96th',
    'ninety sixth': '96th', 'ninetysixth': '96th',
    'ninety-seventh': '97th', 'ninety seventh': '97th', 'ninetyseventh': '97th', 'ninety-eighth': '98th',
    'ninety eighth': '98th', 'ninetyeighth': '98th',
    'ninety-ninth': '99th', 'ninety ninth': '99th', 'ninetyninth': '99th', 'hundredth': '100th'
}


def handle_duplicate_columns(df):
    duplicates = df.columns[df.columns.duplicated()]
    if len(duplicates) > 0:
        print(f"Duplicate columns found: {duplicates}")
        # Option 1: Rename duplicate columns
        new_columns = []
        for col in df.columns:
            if col in new_columns:
                new_columns.append(f"{col}_duplicate")
            else:
                new_columns.append(col)
        df.columns = new_columns

        # Option 2: Drop duplicate columns (uncomment if preferred)
        # df = df.loc[:, ~df.columns.duplicated()]

    return df


def preprocess_address(address):
    # Standardize unit labels
    def standardize_unit_label(unit_label):
        unit_label = unit_label.strip().lower()
        if unit_label in ['apt', 'unit', '#', 'suite', 'bldg', 'building']:
            return 'Unit'
        return unit_label

    # 3. Remove the # symbol and dash before numbers (e.g., "Apt #-123" -> "Apt 123")
    address = re.sub(r'#-?\s*(\d+)', r'\1', address)

    # Convert the address to lowercase for consistent processing
    address = address.lower().strip()

    # Handle duplex/triplex/quadruplex pattern: "5800 Hunting Hollow Ct 5802" -> "5800-5802 Hunting Hollow Ct"
    duplex_pattern = re.compile(r'^(\d+)\s+([\w\s]+)\s+(\d+)$', re.IGNORECASE)
    duplex_match = duplex_pattern.match(address)
    if duplex_match:
        num1 = int(duplex_match.group(1))
        num2 = int(duplex_match.group(3))
        street_name = duplex_match.group(2).strip()
        if abs(num1 - num2) in [2, 4,6, 8]:
            print(f"Transforming {address} to {num1}-{num2} {street_name}")
            return f"{num1}-{num2} {street_name}"
        else:
            print(f"Numbers {num1} and {num2} do not differ by 2, 4, or 8. Keeping original format.")
    else:
        print(f"No duplex match found for address: {address}")

    # Handle state route pattern: "1230 - 123 N state Rte" -> "1230 N state Rte 123"
    state_route_pattern = re.compile(r'^(\d+)\s*-\s*(\d+)\s*([NSEW]?)\s*(state\s+rte|state\s+route|state\s+rt)', re.IGNORECASE)
    state_route_match = state_route_pattern.match(address)
    if state_route_match:
        num1 = state_route_match.group(1)
        num2 = state_route_match.group(2)
        direction = state_route_match.group(3).strip()
        route_type = state_route_match.group(4).strip()
        if direction:
            new_address = f"{num1} {direction} {route_type} {num2}"
        else:
            new_address = f"{num1} {route_type} {num2}"
        print(f"Transforming {address} to {new_address}")
        return new_address

    # 4. Convert "456 Maple Ave 34-Unit" to "456 Maple Ave Unit 34"
    address = re.sub(r'(\d+)-unit', r'unit \1', address)

    # 5. State-route adjusting for addresses without direction (already covered above)
    address = re.sub(r'(\d+)-(\d+)\s+(state\s+rte|state\s+route|state\s+rt)', r'\1 \3 \2', address)

    # 7. Remove dash in "123 Main St 12-A" -> "123 Main St 12A"
    address = re.sub(r'(\d+)-([a-zA-Z])$', r'\1\2', address)

    # 8. Remove "Complex A" or "Building B" from any part of the address
    address = re.sub(r',?\s*(complex|building)\s+[a-z]', '', address, flags=re.IGNORECASE)

    # Return the address unchanged if no patterns matched
    return address
def clean_full_zip(zip_code):
    zip_code = str(zip_code).replace(',', '').replace('.0', '')
    return zip_code[:5]  # Only keep the first 5 digits



@lru_cache(maxsize=128)
def get_city_from_zip(zip_code):
    url = f"http://api.zippopotam.us/us/{zip_code}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'places' in data and len(data['places']) > 0:
            return data['places'][0]['place name']
    return None


async def fetch_city(session, zip_code):
    url = f"http://api.zippopotam.us/us/{zip_code}"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            if 'places' in data and len(data['places']) > 0:
                return zip_code, data['places'][0]['place name']
    return zip_code, None


async def fetch_city_map_async(zip_codes, max_concurrent_tasks=100):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(max_concurrent_tasks)

        async def fetch_with_sem(zip_code):
            async with semaphore:
                return await fetch_city(session, zip_code)

        tasks = [fetch_with_sem(zip_code) for zip_code in zip_codes]
        results = await asyncio.gather(*tasks)
        return {zip_code: city for zip_code, city in results}


def fetch_city_map(zip_codes):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    city_map = loop.run_until_complete(fetch_city_map_async(zip_codes))
    return city_map


# Column mapping configuration with variations
column_mapping_config = {
    'property_address': ['property address', 'address', 'property_address', 'site address', "Street", 'street_address'],
    'property_city': ['property city', 'city', 'property_city'],
    'property_state': ['property state', 'state', 'property_state', "region"],
    'property_zip': ['property zip', 'property zipcode', 'zip', 'zipcode', 'property_zip', 'property_zipcode',
                     'zip code', "PostalCode", "postal_code"],
    'mailing_address': ['mailing address', 'owner address', 'mailing_address', 'owner_address'],
    'mailing_city': ['mailing city', 'owner city', 'mailing_city', 'owner_city'],
    'mailing_state': ['mailing state', 'owner state', 'mailing_state', 'owner_state'],
    'mailing_zip': ['mailing zip', 'mailing zipcode', 'owner zip', 'owner zipcode', 'mailing_zip', 'mailing_zipcode',
                    'owner_zip', 'owner_zipcode'],
    'full_name': ['full name', 'owner full name', 'first owner full name', 'full_name', 'owner_full_name',
                  'first_owner_full_name', 'owner contact name'],
    'first_name': ['first name', 'owner first name', 'first owner first name', 'first_name', 'owner_first_name',
                   'first_owner_first_name'],
    'last_name': ['last name', 'owner last name', 'first owner last name', 'last_name', 'owner_last_name',
                  'first_owner_last_name']
}

# Function to standardize column names for easier matching
def standardize_column_name(name):
    return re.sub(r'[\s_]+', ' ', name.strip().lower())

# Function to convert text to title case
def to_title_case(text):
    if isinstance(text, str):
        # Split the text into words and capitalize the first letter of each word
        return ' '.join(word.capitalize() for word in text.split())
    return text

# Function to create a standardized column map from the DataFrame columns
def create_standardized_column_map(df_columns):
    return {re.sub(r'[\s_]+', ' ', col.strip().lower()): col for col in df_columns}

# Function to map columns automatically
def map_columns(df, config, standardized_columns_map):
    mapped_columns = {}
    for key, possible_names in config.items():
        for name in possible_names:
            standardized_name = re.sub(r'[\s_]+', ' ', name.strip().lower())
            if standardized_name in standardized_columns_map:
                mapped_columns[key] = standardized_columns_map[standardized_name]
                break
        else:
            mapped_columns[key] = 'none'
    return mapped_columns

# Function to adjust cities based on ZIP codes
def adjust_cities(df, mapped_columns):
    def clean_zip(zip_code):
        return str(zip_code).replace(',', '').replace('.0', '')

    if mapped_columns['property_zip'] != 'none' and mapped_columns['property_city'] != 'none':
        property_zip_col = mapped_columns['property_zip']
        property_city_col = mapped_columns['property_city']

        # Clean and convert the property ZIP codes to strings
        df[property_zip_col] = df[property_zip_col].apply(clean_zip)
        df[property_zip_col] = df[property_zip_col].apply(clean_full_zip)
        zip_codes = df[property_zip_col].unique()
        city_map = fetch_city_map(zip_codes)

        df[property_city_col] = df[property_zip_col].map(city_map).fillna(df[property_city_col])

    if mapped_columns['mailing_zip'] != 'none' and mapped_columns['mailing_city'] != 'none':
        mailing_zip_col = mapped_columns['mailing_zip']
        mailing_city_col = mapped_columns['mailing_city']

        # Clean and convert the mailing ZIP codes to strings
        df[mailing_zip_col] = df[mailing_zip_col].apply(clean_zip)
        df[mailing_zip_col] = df[mailing_zip_col].apply(clean_full_zip)
        zip_codes = df[mailing_zip_col].unique()
        city_map = fetch_city_map(zip_codes)

        df[mailing_city_col] = df[mailing_zip_col].map(city_map).fillna(df[mailing_city_col])

    return df

# Function to standardize and normalize address, ensuring the correct replacement of ordinal and address patterns
def standardize_and_normalize_address(address):
    if isinstance(address, str):
        # Remove '#' symbols followed by numbers
        address = re.sub(r'\s*#\s*(?=\d*\s|$)', ' ', address)
        # Convert to lower case
        address = address.lower()
        # Split into words
        words = address.split()

        # Replace multi-word ordinal phrases
        i = 0
        while i < len(words) - 1:
            two_word = f"{words[i]} {words[i + 1]}"
            if two_word in ordinal_mapping:
                words[i] = ordinal_mapping[two_word]
                del words[i + 1]
            else:
                i += 1

        # Replace single-word ordinals
        words = [ordinal_mapping.get(word, word) for word in words]

        # Reconstruct the address after ordinal replacement
        address = ' '.join(words)

        # Replace address patterns
        words = address.split()
        last_index_to_replace = None

        # Track the last index of an address pattern match
        for i, word in enumerate(words):
            for pattern, replacement in address_patterns:
                if pattern.match(word):
                    last_index_to_replace = i

        # Replace the last occurrence of the address pattern
        if last_index_to_replace is not None:
            for pattern, replacement in address_patterns:
                if pattern.match(words[last_index_to_replace]):
                    words[last_index_to_replace] = pattern.sub(replacement, words[last_index_to_replace])
                    break

        # Replace directional patterns unless followed by an address pattern
        for i, word in enumerate(words):
            for pattern, replacement in directional_patterns.items():
                if pattern.match(word):
                    if i + 1 < len(words):
                        next_word = words[i + 1]
                        if any(addr_pattern.match(next_word) for addr_pattern, _ in address_patterns):
                            continue
                    words[i] = pattern.sub(replacement, words[i])
                    break

        # Reconstruct the address
        address = ' '.join(words)

    return address

# Streamlit app starts here
st.title("Address Normalization and ZIP Code Adjustment")

# File upload and reading
uploaded_file = st.file_uploader("Upload your CSV or Excel file", type=['csv', 'xlsx'])

if uploaded_file is not None:
    try:
        # Convert the uploaded file to a format that can be used by pandas
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(uploaded_file.getvalue()), encoding='utf-8')
        elif uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(uploaded_file.getvalue()))

        st.write("File Uploaded Successfully")

        # Create the standardized column map after df is defined
        standardized_columns_map = create_standardized_column_map(df.columns)

        # Map columns automatically using the standardized map
        mapped_columns = map_columns(df, column_mapping_config, standardized_columns_map)

        # Filter the DataFrame to keep only the relevant columns
        df_filtered = df[[mapped_columns[key] for key in mapped_columns if mapped_columns[key] != 'none']]

        if df_filtered.empty:
            st.error("No relevant columns found after mapping.")
        else:
            st.write("Filtered DataFrame:", df_filtered.head())

    except Exception as e:
        st.error(f"Failed to process file: {e}")

    # Ensure df is defined before proceeding
    if 'df' in locals():
        # Create standardized column map after df is defined
        standardized_columns_map = create_standardized_column_map(df.columns)

        # Use the standardized map to map columns
        mapped_columns = map_columns(df, column_mapping_config, standardized_columns_map)

        # Continue with further processing...
        st.write("Mapped Columns: ", mapped_columns)

        # Allow the user to adjust the mappings
        st.write("Please confirm or adjust the column mappings:")
        for key in column_mapping_config.keys():
            options = ['none'] + list(df.columns)
            default_index = options.index(mapped_columns.get(key)) if mapped_columns.get(key) in options else 0
            mapped_columns[key] = st.selectbox(f"Select column for {key.replace('_', ' ').title()}:", options,
                                               index=default_index)

        # Standardize button
        if st.button("Standardize"):
            # Apply the functions to the addresses
            if mapped_columns.get('property_address') != 'none':
                property_address_col = mapped_columns['property_address']
                df[property_address_col].fillna('', inplace=True)
                df[property_address_col] = df[property_address_col].apply(preprocess_address)
                df[property_address_col] = df[property_address_col].apply(standardize_and_normalize_address)

            if mapped_columns.get('mailing_address') != 'none':
                mailing_address_col = mapped_columns['mailing_address']
                df[mailing_address_col].fillna('', inplace=True)
                df[mailing_address_col] = df[mailing_address_col].apply(standardize_and_normalize_address)

            # Adjust cities
            df = adjust_cities(df, mapped_columns)

            # Convert relevant columns to title case if they exist
            for key in ['full_name', 'first_name', 'last_name']:
                if key in mapped_columns and mapped_columns[key] != 'none':
                    df[mapped_columns[key]] = df[mapped_columns[key]].apply(to_title_case)

            st.write("Addresses Normalized and Names Converted to Name Case Successfully")
            st.write(df.head())
            if 'Parcel number' in df.columns:
                parcel_hits_df = df.dropna(subset=['Parcel number'])
                non_parcel_hits_df = df[~df.index.isin(parcel_hits_df.index)]
                print("Processing non-parcel hits")

                # Update records in parcel_hits_df from cache based on Parcel number
                parcel_hits_df = update_records_from_cache(parcel_hits_df, rei)

                # Phase 2: Process remaining records without Parcel numbers

            else:
                # If 'Parcel number' column doesn't exist, treat all records as non-parcel hits
                non_parcel_hits_df = df.copy()

            # Phase 2: Process remaining records without Parcel numbers
            property_columns = {
                'property_address': mapped_columns['property_address'],
                'property_city': mapped_columns['property_city'],
                'property_state': mapped_columns['property_state'],
                'property_zip': mapped_columns['property_zip']
            }


            non_parcel_hits_df['id_num'] = non_parcel_hits_df.index.astype(str)
            non_parcel_hits_df['iso_country_code'] = 'us'

            column_map = {
                'id_num': 'query_id',
                property_columns['property_address']: 'street_address',
                property_columns['property_city']: 'city',
                property_columns['property_state']: 'region',
                property_columns['property_zip']: 'postal_code',
                'iso_country_code': 'iso_country_code'
            }

            # Define the expected columns
            expected_columns = {'query_id', 'region', 'location_name', 'city', 'iso_country_code', 'place_metadata',
                                'street_address', 'latitude', 'longitude', 'postal_code'}

            # Step 1: Rename columns as per the provided mapping
            df_place = non_parcel_hits_df.rename(columns=column_map)
            if property_columns['property_address'] != 'none':
                non_parcel_hits_df.rename(columns={property_columns['property_address']: 'street_address',
                                   property_columns['property_city']: 'city',
                                    property_columns['property_state']:'region',
                                    property_columns['property_zip']:'postal_code'}, inplace=True)

            # Step 2: Ensure that only the expected columns are included
            df_place = df_place[[col for col in expected_columns if col in df_place.columns]]

            # Step 3: Handle duplicate columns (if any)
            df_place = handle_duplicate_columns(df_place)

            # Step 4: Validate columns - Check for missing or extra columns
            missing_columns = expected_columns - set(df_place.columns)
            extra_columns = set(df_place.columns) - expected_columns

            if missing_columns:
                print(f"Missing columns: {missing_columns}")
            if extra_columns:
                print(f"Extra columns: {extra_columns}")
                # Optionally, remove extra columns if found
                df_place = df_place.drop(columns=list(extra_columns))

            # Step 5: Convert to JSON
            data_jsoned = json.loads(df_place.to_json(orient='records'))

            # Provide a text input for the user to specify the file name
            file_name, file_extension = os.path.splitext(uploaded_file.name)
            output_file_name = f"{file_name}_standardized.csv"

            # Separate cached responses and new API requests
            cached_responses, api_requests = check_cache(data_jsoned, cache_df)

            # Proceed with Placekey API lookup for uncached addresses
            if api_requests:
                print("Starting Placekey API lookup...")
                responses = pk_api.lookup_placekeys(api_requests, verbose=True)
            else:
                responses = []

            responses_cleaned = clean_api_responses(api_requests, responses)
            print(responses_cleaned)

            # Immediately cache the valid API responses
            if responses_cleaned:
                new_cache_entries = pd.DataFrame(responses_cleaned)

                # Merge the new cache entries with the cache file
                new_cache_entries = pd.merge(new_cache_entries, non_parcel_hits_df, left_on='query_id',
                                             right_on='id_num',
                                             how='left')

                # new_cache_entries = new_cache_entries[['placekey', 'street_address', 'city', 'region', 'postal_code']]
                # frames = [new_cache_entries, cache_df]
                # cache_df = pd.concat(frames).drop_duplicates(subset=[ 'street_address', 'city'],
                #                                               keep='first')

                # # Save the updated cache file
                # cache_df.to_csv(cache_file_path, index=False)
                print("Cache updated immediately after API response processing.")

            # Combine cached responses with API responses for further processing
            all_responses = cached_responses + responses_cleaned

            # Convert to DataFrame using StringIO to avoid future warning
            df_placekeys = pd.read_json(StringIO(json.dumps(all_responses)), dtype={'query_id': str})

            # Merge the API results with the original non-parcel hits to get complete addresses
            df_join_placekey = pd.merge(non_parcel_hits_df, df_placekeys, left_on='id_num', right_on='query_id',
                                        how='left')

            # Log any records that are still missing placekeys after merging
            missing_placekeys = df_join_placekey['placekey'].isna().sum()
            if missing_placekeys > 0:
                print(f"Warning: {missing_placekeys} records are still missing placekeys after the merge.")
                missing_records_df = df_join_placekey[df_join_placekey['placekey'].isna()]
                missing_records_df.to_csv("missing_records.csv", index=False)
                print("Missing records written to 'missing_records.csv' for further analysis.")

            # Update the records with the placekeys from the cache
            df_join_placekey = update_records_with_placekeys(df_join_placekey, rei)

            # Ensure 'iso_country_code' is carried over
            df_join_placekey['iso_country_code'] = non_parcel_hits_df['iso_country_code']

            # Drop the id_num and query_id columns
            df_join_placekey = df_join_placekey.drop(columns=['id_num', 'query_id'])

            # Combine with the records from Parcel number hits
            if 'parcel_hits_df' in locals() and not parcel_hits_df.empty:
                df_final = pd.concat([parcel_hits_df, df_join_placekey])
            else:
                df_final = df_join_placekey.copy()
            # Combine with the records from Parcel number hits

            df_final_filtered = filter_franklin_county(df_final, rei)

            # Output the final updated DataFrame
            df_final_filtered = df_final_filtered.drop_duplicates(subset=['street_address'])

            # new_records = df_final_filtered[['placekey', 'street_address', 'city', 'region', 'postal_code']]
            # frames = [new_records, rei]
            # rei = pd.concat(frames).drop_duplicates(subset=['street_address', 'city'],
            #                                              keep='first')
            # rei.to_csv(REI_local_path,index=False)
            df_final_filtered.rename(columns={'street_address': 'Property Address',
                                               'city': 'Property City',
                                               'region': 'Property State',
                                               'postal_code': 'Property Zip'}, inplace=True)

            print('Summary of results:')
            total_recs = df_final_filtered.shape[0]
            print(f'Total records after filtering: {total_recs}')
            df_final_filtered= df_final_filtered.drop(['placekey'],axis=1)
            # Provide download link for the updated file
            csv = df_final_filtered.to_csv(index=False).encode('utf-8')
            st.download_button("Download Updated File", data=csv, file_name=output_file_name, mime="text/csv")

            # Instruction for moving the file
            st.markdown("""
                **Instructions:**
                - After downloading, you can manually move the file to your desired location.
                - To move the file, use your file explorer and drag the downloaded file to the preferred folder.
            """)
        if st.button("Fix tag and Standardize"):
            df.rename(columns={'Number Quality Score 3': 'Phone 3 Tags',
                                               'Number Quality Score 2': 'Phone 2 Tags',
                                               'Number Quality Score 1': 'Phone 1 Tags',
                                               }, inplace=True)

            
            # Apply the functions to the addresses
            if mapped_columns.get('property_address') != 'none':
                property_address_col = mapped_columns['property_address']
                df[property_address_col].fillna('', inplace=True)
                df[property_address_col] = df[property_address_col].apply(preprocess_address)
                df[property_address_col] = df[property_address_col].apply(standardize_and_normalize_address)

            if mapped_columns.get('mailing_address') != 'none':
                mailing_address_col = mapped_columns['mailing_address']
                df[mailing_address_col].fillna('', inplace=True)
                df[mailing_address_col] = df[mailing_address_col].apply(standardize_and_normalize_address)

            # Adjust cities
            df = adjust_cities(df, mapped_columns)

            # Convert relevant columns to title case if they exist
            for key in ['full_name', 'first_name', 'last_name']:
                if key in mapped_columns and mapped_columns[key] != 'none':
                    df[mapped_columns[key]] = df[mapped_columns[key]].apply(to_title_case)

            st.write("Addresses Normalized and Names Converted to Name Case Successfully")
            st.write(df.head())
            if 'Parcel number' in df.columns:
                parcel_hits_df = df.dropna(subset=['Parcel number'])
                non_parcel_hits_df = df[~df.index.isin(parcel_hits_df.index)]
                print("Processing non-parcel hits")

                # Update records in parcel_hits_df from cache based on Parcel number
                parcel_hits_df = update_records_from_cache(parcel_hits_df, rei)

                # Phase 2: Process remaining records without Parcel numbers

            else:
                # If 'Parcel number' column doesn't exist, treat all records as non-parcel hits
                non_parcel_hits_df = df.copy()

            # Phase 2: Process remaining records without Parcel numbers
            property_columns = {
                'property_address': mapped_columns['property_address'],
                'property_city': mapped_columns['property_city'],
                'property_state': mapped_columns['property_state'],
                'property_zip': mapped_columns['property_zip']
            }


            non_parcel_hits_df['id_num'] = non_parcel_hits_df.index.astype(str)
            non_parcel_hits_df['iso_country_code'] = 'us'

            column_map = {
                'id_num': 'query_id',
                property_columns['property_address']: 'street_address',
                property_columns['property_city']: 'city',
                property_columns['property_state']: 'region',
                property_columns['property_zip']: 'postal_code',
                'iso_country_code': 'iso_country_code'
            }

            # Define the expected columns
            expected_columns = {'query_id', 'region', 'location_name', 'city', 'iso_country_code', 'place_metadata',
                                'street_address', 'latitude', 'longitude', 'postal_code'}

            # Step 1: Rename columns as per the provided mapping
            df_place = non_parcel_hits_df.rename(columns=column_map)
            if property_columns['property_address'] != 'none':
                non_parcel_hits_df.rename(columns={property_columns['property_address']: 'street_address',
                                   property_columns['property_city']: 'city',
                                    property_columns['property_state']:'region',
                                    property_columns['property_zip']:'postal_code'}, inplace=True)

            # Step 2: Ensure that only the expected columns are included
            df_place = df_place[[col for col in expected_columns if col in df_place.columns]]

            # Step 3: Handle duplicate columns (if any)
            df_place = handle_duplicate_columns(df_place)

            # Step 4: Validate columns - Check for missing or extra columns
            missing_columns = expected_columns - set(df_place.columns)
            extra_columns = set(df_place.columns) - expected_columns

            if missing_columns:
                print(f"Missing columns: {missing_columns}")
            if extra_columns:
                print(f"Extra columns: {extra_columns}")
                # Optionally, remove extra columns if found
                df_place = df_place.drop(columns=list(extra_columns))

            # Step 5: Convert to JSON
            data_jsoned = json.loads(df_place.to_json(orient='records'))

            # Provide a text input for the user to specify the file name
            file_name, file_extension = os.path.splitext(uploaded_file.name)
            output_file_name = f"{file_name}_Tagged and standardized.csv"

            # Separate cached responses and new API requests
            cached_responses, api_requests = check_cache(data_jsoned, cache_df)

            # Proceed with Placekey API lookup for uncached addresses
            if api_requests:
                print("Starting Placekey API lookup...")
                responses = pk_api.lookup_placekeys(api_requests, verbose=True)
            else:
                responses = []

            responses_cleaned = clean_api_responses(api_requests, responses)
            print(responses_cleaned)

            # Immediately cache the valid API responses
            if responses_cleaned:
                new_cache_entries = pd.DataFrame(responses_cleaned)

                # Merge the new cache entries with the cache file
                new_cache_entries = pd.merge(new_cache_entries, non_parcel_hits_df, left_on='query_id',
                                             right_on='id_num',
                                             how='left')

                new_cache_entries = new_cache_entries[['placekey', 'street_address', 'city', 'region', 'postal_code']]
                # frames = [new_cache_entries, cache_df]
                # cache_df = pd.concat(frames).drop_duplicates(subset=[ 'street_address', 'city'],
                #                                               keep='first')

                # # Save the updated cache file
                # cache_df.to_csv(cache_file_path, index=False)
                print("Cache updated immediately after API response processing.")

            # Combine cached responses with API responses for further processing
            all_responses = cached_responses + responses_cleaned

            # Convert to DataFrame using StringIO to avoid future warning
            df_placekeys = pd.read_json(StringIO(json.dumps(all_responses)), dtype={'query_id': str})

            # Merge the API results with the original non-parcel hits to get complete addresses
            df_join_placekey = pd.merge(non_parcel_hits_df, df_placekeys, left_on='id_num', right_on='query_id',
                                        how='left')

            # Log any records that are still missing placekeys after merging
            missing_placekeys = df_join_placekey['placekey'].isna().sum()
            if missing_placekeys > 0:
                print(f"Warning: {missing_placekeys} records are still missing placekeys after the merge.")
                missing_records_df = df_join_placekey[df_join_placekey['placekey'].isna()]
                missing_records_df.to_csv("missing_records.csv", index=False)
                print("Missing records written to 'missing_records.csv' for further analysis.")

            # Update the records with the placekeys from the cache
            df_join_placekey = update_records_with_placekeys(df_join_placekey, rei)

            # Ensure 'iso_country_code' is carried over
            df_join_placekey['iso_country_code'] = non_parcel_hits_df['iso_country_code']

            # Drop the id_num and query_id columns
            df_join_placekey = df_join_placekey.drop(columns=['id_num', 'query_id'])

            # Combine with the records from Parcel number hits
            if 'parcel_hits_df' in locals() and not parcel_hits_df.empty:
                df_final = pd.concat([parcel_hits_df, df_join_placekey])
            else:
                df_final = df_join_placekey.copy()
            # Combine with the records from Parcel number hits

            df_final_filtered = filter_franklin_county(df_final, rei)

            # Output the final updated DataFrame
            df_final_filtered = df_final_filtered.drop_duplicates(subset=['street_address'])

            new_records = df_final_filtered[['placekey', 'street_address', 'city', 'region', 'postal_code']]
            frames = [new_records, rei]
            # rei = pd.concat(frames).drop_duplicates(subset=['street_address', 'city'],
            #                                              keep='first')
            # rei.to_csv(REI_local_path,index=False)
            df_final_filtered.rename(columns={'street_address': 'Property Address',
                                               'city': 'Property City',
                                               'region': 'Property State',
                                               'postal_code': 'Property Zip'}, inplace=True)

            print('Summary of results:')
            total_recs = df_final_filtered.shape[0]
            print(f'Total records after filtering: {total_recs}')
            df_final_filtered= df_final_filtered.drop(['placekey'],axis=1)
            # Provide download link for the updated file
            csv = df_final_filtered.to_csv(index=False).encode('utf-8')
            st.download_button("Download Updated File", data=csv, file_name=output_file_name, mime="text/csv")

            # Instruction for moving the file
            st.markdown("""
                **Instructions:**
                - After downloading, you can manually move the file to your desired location.
                - To move the file, use your file explorer and drag the downloaded file to the preferred folder.
            """)
    else:
        st.error("Required columns are missing in the uploaded file.")
